"""Schema 演进领域服务：管理湖表 Schema 的可控演进、兼容性检查和回滚。

上游依赖快照版本模块提供变更前快照，下游联动回填任务进行存量数据迁移。
兼容性检查是核心：变更类型与兼容级别必须匹配，不兼容变更需要回填。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


# 合法兼容级别及其允许的变更类型
COMPATIBILITY_RULES: dict[str, set[str]] = {
    "向后兼容": {"添加字段", "修改默认值", "添加索引"},
    "向前兼容": {"删除字段", "放宽约束"},
    "不兼容": {"修改字段类型", "重命名字段", "删除字段"},
}


class SchemaEvolutionService(BaseModuleService):
    MODULE = "schema_evolution"
    ENTITY = "Schema变更"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["湖表名", "变更类型", "变更字段", "兼容级别", "变更前快照"]
    REQUIRED_FIELDS = ["湖表名", "变更类型", "变更字段"]
    STATUS_ORDER = ["草稿", "待审批", "已执行", "已回滚"]
    ACTION_RULES = {"提交审批": "待审批", "执行变更": "已执行"}
    NEGATIVE_ACTIONS = []

    def submit_approval(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """提交审批：执行兼容性检查。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "Schema 变更不存在"
        if entry.get("status") != "草稿":
            return None, f"Schema 变更状态为{entry.get('status')}，不可提交审批"

        change_type = entry.get("变更类型", "")
        compat_level = entry.get("兼容级别", "")

        # 兼容性检查
        allowed = COMPATIBILITY_RULES.get(compat_level, set())
        if change_type not in allowed:
            return (
                None,
                f"兼容性检查失败：{compat_level} 不允许 {change_type}，"
                f"允许的操作：{'、'.join(allowed) if allowed else '无'}",
            )

        # 检查变更前快照是否可用
        snapshot_ref = entry.get("变更前快照", "")
        if snapshot_ref:
            snapshots = store.find_by_field("snapshot_version", "版本号", snapshot_ref)
            available = [s for s in snapshots if s.get("status") == "可用"]
            if not available:
                return None, f"变更前快照 {snapshot_ref} 不可用"

        entry["status"] = "待审批"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("schema_submitted", "schema_evolution", "snapshot_version", {
            "entry_id": entry_id,
            "table_name": entry.get("湖表名", ""),
            "change_type": change_type,
            "compat_level": compat_level,
        })

        return entry, f"Schema 变更 {entry.get('湖表名')} - {change_type} 已提交审批，兼容级别={compat_level}"

    def execute_change(
        self,
        entry_id: int,
        columns: list[Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """执行 Schema 变更：联动回填任务进行存量数据迁移。

        不兼容变更会把确认后的字段定义（列名和类型）随变更事件传给回填，
        回填侧登记为回填字段版本记录，中断恢复时据此核对下一段结构。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "Schema 变更不存在"
        if entry.get("status") != "待审批":
            return None, f"Schema 变更状态为{entry.get('status')}，不可执行"

        table_name = entry.get("湖表名", "")
        change_type = entry.get("变更类型", "")
        compat_level = entry.get("兼容级别", "")

        # 自身状态流转
        entry["status"] = "已执行"
        entry["pending"] = False
        entry["abnormal"] = False

        # 如果是不兼容变更，触发回填任务
        if compat_level == "不兼容":
            backfill_jobs = store.find_by_field("backfill_job", "目标湖表", table_name)
            if backfill_jobs:
                for bj in backfill_jobs:
                    if bj.get("status") == "待调度":
                        bj["pending"] = True

            # 变更后的列定义：以执行时确认的定义为准，缺省按变更字段生成
            change_columns = columns or [
                {"name": entry.get("变更字段", ""), "type": "待确认"}
            ]

            event = store.log_event("schema_to_backfill", "schema_evolution", "backfill_job", {
                "entry_id": entry_id,
                "table_name": table_name,
                "change_type": change_type,
                "compat_level": compat_level,
                "columns": change_columns,
                "affected_jobs": len(backfill_jobs),
            })

            # 字段变更事件同步登记为回填字段版本记录，恢复时可从确认记录追查
            from app.services.backfill_job import BackfillJobService
            BackfillJobService().apply_schema_change(
                table_name,
                change_columns,
                source=f"schema_event#{event['id']}",
                confirmed_by=f"schema_evolution#{entry_id}",
                event_id=event["id"],
            )

        store.log_event("schema_executed", "schema_evolution", "lake_table", {
            "entry_id": entry_id,
            "table_name": table_name,
            "change_type": change_type,
        })

        return entry, f"Schema 变更 {table_name} - {change_type} 已执行"

    def rollback_change(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """回滚 Schema 变更：恢复到变更前快照。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "Schema 变更不存在"
        if entry.get("status") != "已执行":
            return None, f"Schema 变更状态为{entry.get('status')}，不可回滚"

        snapshot_ref = entry.get("变更前快照", "")
        table_name = entry.get("湖表名", "")

        # 尝试恢复快照
        if snapshot_ref:
            # 查找该类快照
            for key in store.snapshot_keys():
                if snapshot_ref in key and table_name in key:
                    entry["status"] = "已回滚"
                    entry["pending"] = False
                    entry["abnormal"] = False
                    store.log_event("schema_rolled_back", "schema_evolution", "snapshot_version", {
                        "entry_id": entry_id,
                        "table_name": table_name,
                        "snapshot_ref": snapshot_ref,
                    })
                    return entry, f"Schema 变更回滚到快照 {snapshot_ref}"

        entry["status"] = "已回滚"
        entry["pending"] = False
        entry["abnormal"] = True
        return entry, f"Schema 变更已回滚（快照 {snapshot_ref} 无法精确恢复）"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "提交审批":
            return self.submit_approval(entry_id)
        if action == "执行变更":
            return self.execute_change(entry_id)
        return super().run_action(entry_id, action)