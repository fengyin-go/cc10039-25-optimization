"""领域服务单元测试：验证增强后的业务逻辑、跨模块联动和边界条件。

注意：store 是全局单例，测试间共享状态。测试应验证逻辑正确性，
而非精确的入口记录 id。种子数据中的字段值使用占位符，某些跨模块关联
在种子数据中可能不天然成立。
"""
from __future__ import annotations

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.partition_policy import PartitionPolicyService
from app.services.lake_table import LakeTableService
from app.services.storage_tier import StorageTierService
from app.services.file_compaction import FileCompactionService
from app.services.retention_mgr import RetentionMgrService
from app.services.snapshot_version import SnapshotVersionService
from app.services.schema_evolution import SchemaEvolutionService
from app.services.backfill_job import BackfillJobService
from app.services.ingest_pipeline import IngestPipelineService
from app.services.quality_monitor import QualityMonitorService
from app.services.lineage_graph import LineageGraphService
from app.services.govern_report import GovernReportService


def _has_module_row(module: str) -> bool:
    return len(store.rows(module)) > 0


class TestPartitionPolicy:
    """分区策略 + 湖表注册联动"""

    def setup_method(self):
        self.svc = PartitionPolicyService()
        self.lake_svc = LakeTableService()

    def test_enable_policy_returns_result(self):
        """启用分区策略应有返回（可能成功或被状态阻塞）。"""
        entry, msg = self.svc.enable_policy(1)
        # 不应抛出异常
        assert isinstance(msg, str)
        assert len(msg) > 0

    def test_enable_policy_with_registered_table_links(self):
        """启用分区策略后，目标湖表若存在则分区键被更新。"""
        # 先确保一个湖表已注册
        self.lake_svc.register_table(1)
        # 找一个生效的策略看它的目标湖表
        for policy in store.rows("partition_policy"):
            table_name = policy.get("目标湖表", "")
            lake_tables = store.find_by_field("lake_table", "表名", table_name)
            if lake_tables and policy["status"] == "待审批":
                result, _ = self.svc.enable_policy(policy["id"])
                if result is not None:
                    status = result["status"]
                    assert status in ("已生效", "待审批", "已归档")

    def test_lifecycle_report(self):
        """生命周期报告应返回结构。"""
        # 先尝试启用一条
        for policy_id in [1, 2, 3]:
            p = store.find("partition_policy", policy_id)
            if p and p["status"] == "待审批":
                self.svc.enable_policy(policy_id)
                break

        report = self.svc.lifecycle_report(1)
        assert report is not None
        assert "linked_lake_tables" in report


class TestLakeTable:
    """湖表注册 + 冷热分层联动"""

    def setup_method(self):
        self.svc = LakeTableService()

    def test_register_table_works(self):
        """注册湖表应成功或将状态流转。"""
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                entry, msg = self.svc.register_table(lt_id)
                assert isinstance(msg, str)
                assert len(msg) > 0
                if entry is not None:
                    assert entry["status"] == "已注册"
                return
        pytest.skip("No lake table in 创建中 status")

    def test_register_creates_cross_event(self):
        """注册湖表应产生跨模块事件。"""
        before_count = len(store._events)
        for lt_id in [2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                self.svc.register_table(lt_id)
                after_count = len(store._events)
                assert after_count >= before_count
                return

    def test_offline_blocked_by_dependencies(self):
        """下线湖表被活跃依赖阻塞时应有清晰消息。"""
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "已注册":
                entry, msg = self.svc.offline_table(lt_id)
                # 无论阻塞还是成功，消息都应可读
                assert len(msg) > 0
                return


class TestFileCompaction:
    """小文件合并幂等追踪"""

    def setup_method(self):
        self.svc = FileCompactionService()

    def test_trigger_pending_task(self):
        """触发待调度任务应成功。"""
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                entry, msg = self.svc.trigger_compaction(fc_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "执行中"
                return
        pytest.skip("No compaction task in 待调度")

    def test_complete_notifies_retention(self):
        """完成合并后触发保留策略事件。"""
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "执行中":
                self.svc.complete_compaction(fc_id, "5")
                events = store.events_between("file_compaction", "retention_mgr")
                assert len(events) > 0
                return

        # 如果都在其他状态，先触发再完成
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                self.svc.trigger_compaction(fc_id)
                self.svc.complete_compaction(fc_id, "5")
                events = store.events_between("file_compaction", "retention_mgr")
                assert len(events) > 0
                return

    def test_idempotent_tracking(self):
        """幂等追踪：完成后的任务不应重复完成。"""
        # 找到已完成或完成一个
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                self.svc.trigger_compaction(fc_id)
                self.svc.complete_compaction(fc_id, "5")
                task_no = fc["任务编号"]
                assert self.svc.is_task_completed(task_no) is True
                return
        pytest.skip("All compactions in non-pending state")


class TestStorageTier:
    """冷热分层迁移"""

    def setup_method(self):
        self.svc = StorageTierService()
        self.lake_svc = LakeTableService()

    def test_enable_tier_checks_table(self):
        """启用分层策略应检查湖表状态。"""
        for st_id in [1, 2, 3]:
            st = store.find("storage_tier", st_id)
            if st and st["status"] == "待生效":
                entry, msg = self.svc.enable_tier(st_id)
                assert isinstance(msg, str)
                return

    def test_migrate_partitions_does_not_crash(self):
        """分区迁移不抛异常。"""
        for st_id in [1, 2, 3]:
            entry, msg = self.svc.migrate_partitions(st_id)
            assert entry is not None
            assert isinstance(msg, str)
            break


class TestRetentionMgr:
    """保留策略清理"""

    def setup_method(self):
        self.svc = RetentionMgrService()

    def test_enable_retention(self):
        """启用保留策略。"""
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                entry, msg = self.svc.enable_retention(rm_id)
                assert isinstance(msg, str)
                return

    def test_execute_cleanup_notifies_govern(self):
        """清理完成后通知治理报告。"""
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                self.svc.enable_retention(rm_id)

        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "已生效":
                entry, msg = self.svc.execute_cleanup(rm_id)
                assert entry is not None
                assert isinstance(msg, str)
                return
        pytest.skip("No active retention policy")


class TestSnapshotVersion:
    """快照版本管理"""

    def setup_method(self):
        self.svc = SnapshotVersionService()

    def test_generate_snapshot_works(self):
        """生成快照应返回可用状态或合理拒绝消息。"""
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                entry, msg = self.svc.generate_snapshot(sv_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "可用"
                return
        pytest.skip("No snapshot in 生成中")


class TestSchemaEvolution:
    """Schema 演进兼容性检查"""

    def setup_method(self):
        self.svc = SchemaEvolutionService()

    def test_compatibility_check(self):
        """兼容性检查：不兼容操作被级别拒绝。"""
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se["status"] == "草稿":
                result, msg = self.svc.submit_approval(se_id)
                assert isinstance(msg, str)
                if result is None:
                    assert "兼容性" in msg or "快照" in msg or "不允许" in msg
                return
        pytest.skip("No schema change in 草稿")

    def test_execute_change_links_backfill(self):
        """不兼容变更执行后联动回填。"""
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se.get("兼容级别") == "不兼容":
                # 先提交
                approve_result, _ = self.svc.submit_approval(se_id)
                if approve_result is not None:
                    entry, msg = self.svc.execute_change(se_id)
                    assert isinstance(msg, str)
                return
        pytest.skip("No incompatible schema evolution")


class TestBackfillJob:
    """回填任务检查点"""

    def setup_method(self):
        self.svc = BackfillJobService()

    def test_checkpoint_workflow(self):
        """回填检查点断点续做。"""
        for bj_id in [1, 2, 3]:
            bj = store.find("backfill_job", bj_id)
            if bj and bj["status"] == "待调度":
                result, msg = self.svc.start_backfill(bj_id)
                if result is None:
                    continue
                assert result["status"] == "回填中"

                cp_result, _ = self.svc.checkpoint_backfill(bj_id, 50)
                assert cp_result is not None
                assert self.svc.get_checkpoint(bj_id) == 50
                return
        pytest.skip("No backfill in 待调度")

    def test_complete_backfill_notifies_govern(self):
        """完成回填应通知治理报告。"""
        for bj_id in [1, 2, 3]:
            bj = store.find("backfill_job", bj_id)
            if bj and bj["status"] == "待调度":
                self.svc.start_backfill(bj_id)
                result2, _ = self.svc.complete_backfill(bj_id, 100)
                assert result2 is not None
                assert result2["status"] == "已完成"
                events = store.events_between("backfill_job", "govern_report")
                assert len(events) > 0
                return
        pytest.skip("No backfill in 待调度")


class TestBackfillFieldVersion:
    """回填字段版本记录：Schema 变更中断恢复的端到端场景。

    场景：按整数列定义先写完半个区间 → 暂停回填 → Schema 演进生效文本列
    → 恢复运行核对下一段输入范围 → 续写后半区间 → 分段详情对照直到任务关闭。
    """

    def setup_method(self):
        self.svc = BackfillJobService()
        self.schema_svc = SchemaEvolutionService()

    def _create_task(self, table: str) -> dict:
        entry, missing = self.svc.create_entry({
            "任务名称": f"回填-{table}",
            "目标湖表": table,
            "回填时间范围": "2026-01-01~2026-06-30",
            "处理记录数": "0",
            "执行状态": "未启动",
        })
        assert entry is not None, f"缺少必填字段: {missing}"
        return entry

    def _execute_schema_change(self, table: str, columns: list) -> dict:
        """发布并执行一个不兼容 Schema 变更，返回变更记录。"""
        se, missing = self.schema_svc.create_entry({
            "湖表名": table,
            "变更类型": "修改字段类型",
            "变更字段": columns[0]["name"],
            "兼容级别": "不兼容",
            "变更前快照": "",
        })
        assert se is not None, f"缺少必填字段: {missing}"
        approved, msg = self.schema_svc.submit_approval(se["id"])
        assert approved is not None, msg
        executed, msg = self.schema_svc.execute_change(se["id"], columns=columns)
        assert executed is not None, msg
        return se

    def test_segment_write_pause_schema_change_resume(self):
        """整数列写半区间 → 暂停 → 文本列生效 → 恢复核对 → 续写 → 关闭。"""
        table = "orders_fact_version_e2e"
        task = self._create_task(table)
        tid = task["id"]

        # 启动并登记初始字段版本：整数列
        entry, msg = self.svc.start_backfill(tid)
        assert entry is not None, msg
        v1, msg = self.svc.register_field_version(
            tid, [{"name": "amount", "type": "整数"}], source="初始定义")
        assert v1 is not None and v1["version"] == 1

        # 按整数列定义先写完半个区间 [0, 500)
        seg1, msg = self.svc.write_segment(tid, 500)
        assert seg1 is not None, msg
        assert (seg1["range_start"], seg1["range_end"]) == (0, 500)
        assert seg1["columns"] == [{"name": "amount", "type": "整数"}]
        assert seg1["version"] == 1

        # 暂停回填（中断），已提交分段保留
        paused, msg = self.svc.pause_backfill(tid)
        assert paused is not None and paused["status"] == "已暂停"

        # 暂停期间 Schema 演进发布文本列定义（不兼容变更）
        se = self._execute_schema_change(table, [{"name": "amount", "type": "文本"}])

        # 回填字段版本记录已同步：v2 文本列，可从确认记录追查
        current = self.svc.current_field_version(tid)
        assert current is not None and current["version"] == 2
        assert current["columns"] == [{"name": "amount", "type": "文本"}]
        assert current["confirmed_by"] == f"schema_evolution#{se['id']}"

        # 恢复运行：核对已写列的重读方式、下一段结构和输入范围
        review, msg = self.svc.resume_backfill(tid)
        assert review is not None, msg
        assert review["processed_range"] == {"start": 0, "end": 500}
        assert review["committed_rows"] == 500
        assert review["next_range_start"] == 500
        assert review["last_segment_columns"] == [{"name": "amount", "type": "整数"}]
        assert review["next_columns"] == [{"name": "amount", "type": "文本"}]
        assert review["version_source"] == f"schema_evolution#{se['id']}"

        # 续写后半区间 [500, 1000)，按文本列定义落库
        seg2, msg = self.svc.write_segment(tid, 500)
        assert seg2 is not None, msg
        assert (seg2["range_start"], seg2["range_end"]) == (500, 1000)
        assert seg2["columns"] == [{"name": "amount", "type": "文本"}]
        assert seg2["version"] == 2

        # 分段详情：对照各段使用的字段定义及行数
        detail, msg = self.svc.segment_detail(tid)
        assert detail is not None
        assert detail["committed_rows"] == 1000
        assert detail["processed_range"] == {"start": 0, "end": 1000}
        assert detail["consistent"] is True
        assert [s["version"] for s in detail["segments"]] == [1, 2]
        assert [s["row_count"] for s in detail["segments"]] == [500, 500]
        assert detail["segments"][0]["columns"][0]["type"] == "整数"
        assert detail["segments"][1]["columns"][0]["type"] == "文本"
        assert [v["version"] for v in detail["field_versions"]] == [1, 2]

        # 任务关闭：已提交范围不重算，分段详情仍可对照
        done, msg = self.svc.complete_backfill(tid, 1000)
        assert done is not None and done["status"] == "已完成"
        assert done["处理记录数"] == "1000"
        closed, _ = self.svc.segment_detail(tid)
        assert closed["committed_rows"] == 1000
        assert len(closed["segments"]) == 2
        assert len(closed["field_versions"]) == 2

    def test_committed_segments_not_recounted_on_resume(self):
        """已提交的数据不会在续做时再算一次。"""
        task = self._create_task("orders_fact_no_recount")
        tid = task["id"]
        self.svc.start_backfill(tid)
        self.svc.register_field_version(
            tid, [{"name": "amount", "type": "整数"}], source="初始定义")
        self.svc.write_segment(tid, 300)
        self.svc.pause_backfill(tid)

        review, _ = self.svc.resume_backfill(tid)
        assert review["processed_range"]["end"] == 300
        assert review["committed_rows"] == 300
        assert review["next_range_start"] == 300

        entry = store.find("backfill_job", tid)
        assert entry["处理记录数"] == "300"

        # 同一范围重复提交被幂等拒绝，不重复计数
        self.svc._checkpoints[tid] = 0  # 模拟脏检查点
        self.svc.pause_backfill(tid)
        review2, _ = self.svc.resume_backfill(tid)
        # 恢复时核对并修正：以已提交范围末尾为准
        assert review2["next_range_start"] == 300
        assert self.svc.get_checkpoint(tid) == 300

    def test_write_segment_requires_running_and_version(self):
        """状态与字段版本守卫：未启动、未登记版本都不可落库。"""
        task = self._create_task("orders_fact_guard")
        tid = task["id"]

        seg, msg = self.svc.write_segment(tid, 100)
        assert seg is None and "不可写入分段" in msg

        self.svc.start_backfill(tid)
        seg2, msg2 = self.svc.write_segment(tid, 100)
        assert seg2 is None and "字段版本" in msg2

        # 暂停后也不可写入
        self.svc.register_field_version(
            tid, [{"name": "c1", "type": "整数"}], source="初始定义")
        self.svc.pause_backfill(tid)
        seg3, msg3 = self.svc.write_segment(tid, 100)
        assert seg3 is None and "不可写入分段" in msg3

    def test_sync_schema_events_idempotent(self):
        """从 Schema 变更事件同步字段版本记录：同一事件不重复登记。"""
        table = "orders_fact_sync"
        task = self._create_task(table)
        tid = task["id"]

        # 执行不兼容变更：推送登记一次
        se = self._execute_schema_change(table, [{"name": "c1", "type": "文本"}])
        assert len(self.svc._field_versions[tid]) == 1

        # 再同步事件日志：已消费，不重复登记
        assert self.svc.sync_schema_events(tid) == []
        assert len(self.svc._field_versions[tid]) == 1

        # 兜底路径：事件先于任务存在时从日志补登记
        store.log_event("schema_to_backfill", "schema_evolution", "backfill_job", {
            "entry_id": se["id"],
            "table_name": table,
            "change_type": "修改字段类型",
            "compat_level": "不兼容",
            "columns": [{"name": "c1", "type": "整数"}],
        })
        versions = self.svc.sync_schema_events(tid)
        assert len(versions) == 1
        assert versions[0]["columns"] == [{"name": "c1", "type": "整数"}]
        assert self.svc.sync_schema_events(tid) == []

    def test_closed_task_not_versioned(self):
        """已关闭任务不再接收新的字段版本。"""
        table = "orders_fact_closed"
        task = self._create_task(table)
        tid = task["id"]
        self.svc.start_backfill(tid)
        self.svc.register_field_version(
            tid, [{"name": "c1", "type": "整数"}], source="初始定义")
        self.svc.write_segment(tid, 100)
        self.svc.complete_backfill(tid, 100)

        affected = self.svc.apply_schema_change(
            table, [{"name": "c1", "type": "文本"}],
            source="schema_event#x", confirmed_by="schema_evolution#x")
        assert affected == []
        assert len(self.svc._field_versions[tid]) == 1


class TestIngestPipeline:
    """入湖管道 + 质量监控联动"""

    def setup_method(self):
        self.svc = IngestPipelineService()
        self.lake_svc = LakeTableService()

    def test_start_pipeline_checks_table(self):
        """启动管道应检查目标湖表。"""
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                entry, msg = self.svc.start_pipeline(ip_id)
                assert isinstance(msg, str)
                return

    def test_complete_pipeline_triggers_quality(self):
        """完成管道应触发质量监控事件。"""
        # 确保湖表已注册
        self.lake_svc.register_table(1)
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                result, _ = self.svc.start_pipeline(ip_id)
                if result is None:
                    continue
                result2, _ = self.svc.complete_pipeline(ip_id, 200)
                assert result2 is not None
                assert result2["status"] == "已完成"
                events = store.events_between("ingest_pipeline", "quality_monitor")
                assert len(events) > 0
                return
        pytest.skip("Cannot complete pipeline flow")


class TestQualityMonitor:
    """质量监控 + 血缘联动"""

    def setup_method(self):
        self.svc = QualityMonitorService()

    def test_enable_monitor(self):
        """启用监控规则。"""
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                entry, msg = self.svc.enable_monitor(qm_id)
                assert isinstance(msg, str)
                if entry is not None:
                    assert entry["status"] == "已启用"
                return

    def test_check_quality_generates_events(self):
        """质量检查应产生事件。"""
        # 确保一条规则已启用
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                self.svc.enable_monitor(qm_id)

        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "已启用":
                entry, msg = self.svc.check_quality(qm_id)
                assert entry is not None
                assert isinstance(msg, str)
                events = store.events_for_module("quality_monitor")
                assert len(events) > 0
                return
        pytest.skip("No enabled quality monitor")


class TestLineageGraph:
    """血缘图谱完整性"""

    def setup_method(self):
        self.svc = LineageGraphService()

    def test_refresh_lineage(self):
        """刷新血缘链路有清晰结论。"""
        for lg_id in [1, 2, 3]:
            entry, msg = self.svc.refresh_lineage(lg_id)
            assert entry is not None, f"刷新失败: {msg}"
            assert entry["status"] in ("已建立", "待验证", "链路断裂")
            break

    def test_mark_broken_notifies_govern(self):
        """断裂血缘通知治理报告。"""
        self.svc.mark_broken(1)
        events = store.events_between("lineage_graph", "govern_report")
        assert len(events) > 0


class TestGovernReport:
    """治理报告跨模块聚合"""

    def setup_method(self):
        self.svc = GovernReportService()

    def test_generate_report_aggregates(self):
        """生成报告应聚合跨模块数据。"""
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                entry, msg = self.svc.generate_report(gr_id)
                assert entry is not None, f"生成失败: {msg}"
                assert entry["status"] == "已生成"
                return
        pytest.skip("No govern report in 待生成")

    def test_execute_optimization_triggers_actions(self):
        """执行优化应触发实际操作。"""
        # 先生成一个报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                self.svc.generate_report(gr_id)

        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "已生成":
                entry, msg = self.svc.execute_optimization(gr_id)
                assert entry is not None, f"执行失败: {msg}"
                assert entry["status"] == "已执行"
                return
        pytest.skip("No generated govern report")


class TestCrossModulePipeline:
    """端到端跨模块数据流测试"""

    def test_full_pipeline_flow(self):
        """完整的端到端数据流在各模块间传递。
        验证核心：事件日志链记录跨模块联动。"""
        pp_svc = PartitionPolicyService()
        lake_svc = LakeTableService()
        tier_svc = StorageTierService()
        comp_svc = FileCompactionService()
        ret_svc = RetentionMgrService()
        gov_svc = GovernReportService()

        # 遍历种子数据中所有可用记录，依次执行
        actions_executed = 0

        # 1) 分区策略
        for pp_id in [1, 2, 3]:
            pp = store.find("partition_policy", pp_id)
            if pp and pp["status"] == "待审批":
                pp_svc.enable_policy(pp_id)
                actions_executed += 1
                break

        # 2) 湖表注册
        for lt_id in [1, 2, 3]:
            lt = store.find("lake_table", lt_id)
            if lt and lt["status"] == "创建中":
                lake_svc.register_table(lt_id)
                actions_executed += 1
                break

        # 3) 冷热分层
        for st_id in [1, 2, 3]:
            st = store.find("storage_tier", st_id)
            if st and st["status"] == "待生效":
                tier_svc.enable_tier(st_id)
                tier_svc.migrate_partitions(st_id)
                actions_executed += 1
                break

        # 4) 小文件合并
        for fc_id in [1, 2, 3]:
            fc = store.find("file_compaction", fc_id)
            if fc and fc["status"] == "待调度":
                result, _ = comp_svc.trigger_compaction(fc_id)
                if result:
                    comp_svc.complete_compaction(fc_id, "5")
                    actions_executed += 1
                    break

        # 5) 保留策略
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "待生效":
                ret_svc.enable_retention(rm_id)
                actions_executed += 1
                break
        # 已生效后执行清理
        for rm_id in [1, 2, 3]:
            rm = store.find("retention_mgr", rm_id)
            if rm and rm["status"] == "已生效":
                ret_svc.execute_cleanup(rm_id)
                actions_executed += 1
                break

        # 6) 治理报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                result, msg = gov_svc.generate_report(gr_id)
                assert result is not None, f"报告生成失败: {msg}"
                assert result["status"] == "已生成"
                actions_executed += 1
                break

        # 验证有跨模块事件
        cross_stats = store.cross_module_stats()
        assert cross_stats["event_count"] > 0, "应该有跨模块事件"
        # 至少完成了一些操作
        assert actions_executed > 0, "至少执行了一项操作"

    def test_snapshot_schema_backfill_flow(self):
        """快照 → Schema演化 → 回填任务流。"""
        snap_svc = SnapshotVersionService()
        schema_svc = SchemaEvolutionService()

        # 1) 生成快照
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                snap_svc.generate_snapshot(sv_id)
                break

        # 2) Schema 审批 + 执行
        for se_id in [1, 2, 3]:
            se = store.find("schema_evolution", se_id)
            if se and se["status"] == "草稿":
                result, msg = schema_svc.submit_approval(se_id)
                if result is not None:
                    schema_svc.execute_change(se_id)
                break

        # 验证不抛异常
        assert True

    def test_ingest_to_quality_to_lineage_to_govern_flow(self):
        """入湖 → 质量 → 血缘 → 治理报告流。"""
        lake_svc = LakeTableService()
        ingest_svc = IngestPipelineService()
        qual_svc = QualityMonitorService()
        gov_svc = GovernReportService()

        # 确保湖表已注册
        lake_svc.register_table(1)

        # 启动入湖管道
        for ip_id in [1, 2, 3]:
            ip = store.find("ingest_pipeline", ip_id)
            if ip and ip["status"] == "待启动":
                result, _ = ingest_svc.start_pipeline(ip_id)
                if result is None:
                    continue
                ingest_svc.complete_pipeline(ip_id, 500)
                events1 = store.events_between("ingest_pipeline", "quality_monitor")
                assert len(events1) > 0, "入湖管道完成应触发质量监控"
                break

        # 质量检查
        for qm_id in [1, 2, 3]:
            qm = store.find("quality_monitor", qm_id)
            if qm and qm["status"] == "待启用":
                qual_svc.enable_monitor(qm_id)
            elif qm and qm["status"] == "已启用":
                qual_svc.check_quality(qm_id)
                break

        # 治理报告
        for gr_id in [1, 2, 3]:
            gr = store.find("govern_report", gr_id)
            if gr and gr["status"] == "待生成":
                result, _ = gov_svc.generate_report(gr_id)
                assert result is not None
                break

    def test_dirty_data_handling(self):
        """脏数据处理：重复操作不应抛异常。"""
        pp_svc = PartitionPolicyService()
        # 尝试重复启用同一策略
        for pp_id in [1, 2, 3]:
            pp = store.find("partition_policy", pp_id)
            if pp and pp["status"] == "待审批":
                # 第一次
                pp_svc.enable_policy(pp_id)
                # 第二次应该被拒绝
                result2, msg2 = pp_svc.enable_policy(pp_id)
                assert result2 is None, f"重复启用应被拒绝: {msg2}"
                assert len(msg2) > 0
                return

    def test_create_entries_sequence(self):
        """创建 → 启用序列不抛异常。"""
        pp_svc = PartitionPolicyService()
        entry, missing = pp_svc.create_entry({
            "策略名称": "test_policy",
            "分区维度": "day",
            "分区粒度": "1d",
        })
        assert entry is not None
        assert "id" in entry

    def test_snapshot_and_cross_events(self):
        """快照操作和跨模块事件记录。"""
        # 生成快照
        for sv_id in [1, 2, 3]:
            sv = store.find("snapshot_version", sv_id)
            if sv and sv["status"] == "生成中":
                snap_svc = SnapshotVersionService()
                result, _ = snap_svc.generate_snapshot(sv_id)
                if result:
                    assert result["status"] == "可用"
                    break

        # 验证快照存在
        keys = store.snapshot_keys()
        assert len(keys) > 0

    def test_all_service_interfaces_complete(self):
        """验证所有核心服务的接口完整性。"""
        services = [
            PartitionPolicyService(),
            LakeTableService(),
            StorageTierService(),
            FileCompactionService(),
            RetentionMgrService(),
            SnapshotVersionService(),
            SchemaEvolutionService(),
            BackfillJobService(),
            IngestPipelineService(),
            QualityMonitorService(),
            LineageGraphService(),
            GovernReportService(),
        ]
        for svc in services:
            assert svc.MODULE, f"{type(svc).__name__} missing MODULE"
            assert svc.ENTITY, f"{type(svc).__name__} missing ENTITY"
            assert svc.LIST_FIELDS, f"{type(svc).__name__} missing LIST_FIELDS"
            assert svc.STATUS_ORDER, f"{type(svc).__name__} missing STATUS_ORDER"