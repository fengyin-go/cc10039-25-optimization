"""数据回填任务领域服务：管理历史数据回填、补数和断点续做。

支持回填检查点（checkpoint）：中断后可从上次位置继续，不会重复处理已完成的记录。
上游接收 Schema 演进的不兼容变更事件，下游联动治理报告记录回填结果。

回填字段版本记录：Schema 字段变更事件到达后登记为字段版本（含来源确认记录），
每段实际落库的列名和类型随分段记录持久化；中断恢复时据此核对下一段输入范围，
已提交的分段不会重复统计，任务关闭后仍可在分段详情中对照各段字段定义及行数。
"""
from __future__ import annotations

import datetime
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _normalize_columns(columns: list[Any] | None) -> list[dict[str, str]]:
    """把列定义规范化为 [{name, type}] 结构，便于逐段快照和核对。"""
    normalized: list[dict[str, str]] = []
    for col in columns or []:
        if isinstance(col, dict):
            normalized.append({
                "name": str(col.get("name", "")),
                "type": str(col.get("type", "未指定")),
            })
        else:
            normalized.append({"name": str(col), "type": "未指定"})
    return normalized


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "回填任务"
    OPERATION_TERM = "调度"
    LIST_FIELDS = ["任务名称", "目标湖表", "回填时间范围", "处理记录数", "执行状态"]
    REQUIRED_FIELDS = ["任务名称", "目标湖表", "回填时间范围"]
    STATUS_ORDER = ["待调度", "回填中", "已暂停", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "取消回填": "回填失败"}
    NEGATIVE_ACTIONS = []

    # 回填检查点：任务 id → 已处理记录偏移
    _checkpoints: dict[int, int] = {}
    # 回填字段版本记录：任务 id → 版本记录列表（版本号按登记顺序递增）
    _field_versions: dict[int, list[dict[str, Any]]] = {}
    # 分段落库记录：任务 id → 已提交分段列表（含实际落库的列名和类型）
    _segments: dict[int, list[dict[str, Any]]] = {}
    # 已消费的 Schema 变更事件：任务 id → 事件 id 集合（同一事件不重复登记）
    _consumed_events: dict[int, set[int]] = {}

    # ---- 回填字段版本记录 ----

    def register_field_version(
        self,
        entry_id: int,
        columns: list[Any],
        source: str,
        confirmed_by: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """登记回填字段版本记录：正在使用的列定义及其确认来源。

        source/confirmed_by 记录该版本从哪条确认记录来（如 schema_evolution#3），
        中断后恢复使用的字段定义必须能据此追查。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        normalized = _normalize_columns(columns)
        if not normalized:
            return None, "列定义不能为空，无法登记字段版本"

        versions = self._field_versions.setdefault(entry_id, [])
        record = {
            "version": len(versions) + 1,
            "columns": normalized,
            "source": source,
            "confirmed_by": confirmed_by or source,
            "registered_at": datetime.datetime.now().isoformat(),
        }
        versions.append(record)

        store.log_event("backfill_field_version_registered", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "version": record["version"],
            "columns": normalized,
            "source": source,
            "confirmed_by": record["confirmed_by"],
        })
        return record, f"回填字段版本 v{record['version']} 已登记（来源：{record['confirmed_by']}）"

    def current_field_version(self, entry_id: int) -> dict[str, Any] | None:
        """当前生效的字段版本记录。"""
        versions = self._field_versions.get(entry_id, [])
        return versions[-1] if versions else None

    def apply_schema_change(
        self,
        table_name: str,
        columns: list[Any],
        source: str,
        confirmed_by: str = "",
        event_id: int | None = None,
    ) -> list[dict[str, Any]]:
        """接收 Schema 演进发布的字段定义版本，登记到同表在途回填任务。

        已完成（已关闭）的任务不再变更字段定义；返回受影响任务及新版本号。
        """
        affected: list[dict[str, Any]] = []
        for bj in store.rows(self.MODULE):
            if bj.get("目标湖表") != table_name:
                continue
            if bj.get("status") == "已完成":
                continue
            task_id = int(bj["id"])
            if event_id is not None:
                consumed = self._consumed_events.setdefault(task_id, set())
                if event_id in consumed:
                    continue
                consumed.add(event_id)
            record, _ = self.register_field_version(
                task_id, columns, source=source, confirmed_by=confirmed_by)
            if record is not None:
                affected.append({"task_id": task_id, "version": record["version"]})
        return affected

    def sync_schema_events(self, entry_id: int) -> list[dict[str, Any]]:
        """从 Schema 字段变更事件日志同步回填字段版本记录（幂等）。

        事件先于本功能或推送缺失时兜底：按事件 id 去重，同一事件不重复登记。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return []
        table_name = entry.get("目标湖表", "")
        consumed = self._consumed_events.setdefault(entry_id, set())
        new_versions: list[dict[str, Any]] = []
        for ev in store.events_between("schema_evolution", "backfill_job"):
            if ev["id"] in consumed:
                continue
            payload = ev.get("payload", {})
            if payload.get("table_name") != table_name:
                continue
            columns = payload.get("columns")
            if not columns:
                continue
            consumed.add(ev["id"])
            record, _ = self.register_field_version(
                entry_id,
                columns,
                source=f"schema_event#{ev['id']}",
                confirmed_by=f"schema_evolution#{payload.get('entry_id')}",
            )
            if record is not None:
                new_versions.append(record)
        return new_versions

    # ---- 分段落库与断点续做 ----

    def write_segment(self, entry_id: int, row_count: int) -> tuple[dict[str, Any] | None, str]:
        """把一段数据落库：记下实际写入的列名和类型，并推进检查点。

        每段从已提交范围的末尾接续，同一范围只提交一次，
        已提交的数据续做时不会重复统计。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可写入分段"
        if row_count <= 0:
            return None, "分段行数必须为正整数"

        version = self.current_field_version(entry_id)
        if version is None:
            return None, "尚未登记回填字段版本记录，无法确认落库列定义"

        start = self._checkpoints.get(entry_id, 0)
        end = start + row_count
        segments = self._segments.setdefault(entry_id, [])

        # 幂等：同一输入范围已提交过则直接返回，不重复计数
        for seg in segments:
            if seg["range_start"] == start and seg["range_end"] == end:
                return seg, f"分段 [{start}, {end}) 已提交过，跳过重复写入"

        segment = {
            "segment": len(segments) + 1,
            "range_start": start,
            "range_end": end,
            "row_count": row_count,
            "columns": [dict(c) for c in version["columns"]],
            "version": version["version"],
            "status": "已提交",
            "committed_at": datetime.datetime.now().isoformat(),
        }
        segments.append(segment)
        self._checkpoints[entry_id] = end
        entry["处理记录数"] = str(end)

        store.log_event("backfill_segment_committed", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "segment": segment["segment"],
            "range_start": start,
            "range_end": end,
            "row_count": row_count,
            "version": version["version"],
            "columns": segment["columns"],
        })
        return segment, (
            f"分段 {segment['segment']} 已落库：范围 [{start}, {end})，{row_count} 行，"
            f"字段版本 v{version['version']}"
        )

    def pause_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """暂停回填：保留检查点和已提交分段，等待恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可暂停"

        checkpoint = self._checkpoints.get(entry_id, 0)
        entry["status"] = "已暂停"
        entry["pending"] = True
        entry["abnormal"] = False

        store.log_event("backfill_paused", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "checkpoint": checkpoint,
        })
        return entry, f"回填任务已暂停，检查点保留在 {checkpoint} 条，已提交分段保留"

    def resume_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """恢复回填：从回填字段版本记录核对下一段输入范围。

        返回恢复核对单：上次已落库的列定义（已写好的列该怎样重新读）、
        当前生效的字段版本（下一段会写成什么结构）、已处理范围和下一段起点。
        已提交分段不会重复统计。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") not in ("已暂停", "回填失败"):
            return None, f"回填任务状态为{entry.get('status')}，不可恢复"

        checkpoint = self._checkpoints.get(entry_id, 0)
        segments = self._segments.get(entry_id, [])
        committed_rows = sum(seg["row_count"] for seg in segments)

        # 核对：检查点必须等于已提交分段覆盖的范围末尾，不一致时以已提交范围为准
        if segments and segments[-1]["range_end"] != checkpoint:
            checkpoint = segments[-1]["range_end"]
            self._checkpoints[entry_id] = checkpoint

        version = self.current_field_version(entry_id)
        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处理记录数"] = str(checkpoint)

        review = {
            "task_id": entry_id,
            "processed_range": {"start": 0, "end": checkpoint},
            "committed_rows": committed_rows,
            "next_range_start": checkpoint,
            "last_segment_columns": [dict(c) for c in segments[-1]["columns"]] if segments else None,
            "last_segment_version": segments[-1]["version"] if segments else None,
            "next_columns": [dict(c) for c in version["columns"]] if version else None,
            "next_version": version["version"] if version else None,
            "version_source": version["confirmed_by"] if version else None,
        }
        store.log_event("backfill_resumed", "backfill_job", "backfill_job", review)
        return review, (
            f"回填任务已恢复：已处理范围 [0, {checkpoint})，下一段从 {checkpoint} 开始，"
            f"使用字段版本 v{review['next_version']}（来源：{review['version_source']}）"
        )

    def segment_detail(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """分段详情：对照各段使用的字段定义及行数，直到任务关闭。

        同时核对正在使用的列定义和已处理范围：已提交行数应与范围末尾一致。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"

        segments = self._segments.get(entry_id, [])
        versions = self._field_versions.get(entry_id, [])
        checkpoint = self._checkpoints.get(entry_id, 0)
        committed_rows = sum(seg["row_count"] for seg in segments)
        range_end = segments[-1]["range_end"] if segments else checkpoint

        detail = {
            "task_id": entry_id,
            "task_name": entry.get("任务名称", ""),
            "table_name": entry.get("目标湖表", ""),
            "status": entry.get("status", ""),
            "processed_range": {"start": 0, "end": range_end},
            "committed_rows": committed_rows,
            "consistent": committed_rows == range_end,
            "current_version": versions[-1]["version"] if versions else None,
            "field_versions": [
                {
                    "version": v["version"],
                    "columns": [dict(c) for c in v["columns"]],
                    "source": v["source"],
                    "confirmed_by": v["confirmed_by"],
                    "registered_at": v["registered_at"],
                }
                for v in versions
            ],
            "segments": [
                {
                    "segment": seg["segment"],
                    "range_start": seg["range_start"],
                    "range_end": seg["range_end"],
                    "row_count": seg["row_count"],
                    "columns": [dict(c) for c in seg["columns"]],
                    "version": seg["version"],
                    "status": seg["status"],
                    "committed_at": seg["committed_at"],
                }
                for seg in segments
            ],
        }
        return detail, (
            f"分段详情：{len(segments)} 段已提交，共 {committed_rows} 行，"
            f"已处理范围 [0, {range_end})，当前字段版本 v{detail['current_version']}"
        )

    # ---- 原有生命周期 ----

    def start_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """启动回填任务：检查 Schema 变更状态后启动。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "待调度":
            return None, f"回填任务状态为{entry.get('status')}，不可启动"

        table_name = entry.get("目标湖表", "")
        time_range = entry.get("回填时间范围", "")

        # 检查同湖表是否有执行中的回填
        active_backfills = [
            bj for bj in store.rows(self.MODULE)
            if bj.get("目标湖表") == table_name
            and bj.get("status") == "回填中"
            and bj.get("id") != entry_id
        ]
        if active_backfills:
            return None, f"湖表 {table_name} 已有回填任务在执行中"

        # 初始化检查点
        checkpoint = self._checkpoints.get(entry_id, 0)

        entry["status"] = "回填中"
        entry["pending"] = True
        entry["abnormal"] = False
        entry["处理记录数"] = str(checkpoint)

        store.log_event("backfill_started", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "table_name": table_name,
            "time_range": time_range,
            "checkpoint": checkpoint,
        })

        return entry, f"回填任务 {entry.get('任务名称')} 已启动，时间范围={time_range}，检查点从 {checkpoint} 开始"

    def checkpoint_backfill(self, entry_id: int, processed_count: int) -> tuple[dict[str, Any] | None, str]:
        """更新回填检查点，支持断点续做。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可保存检查点"

        self._checkpoints[entry_id] = processed_count
        entry["处理记录数"] = str(processed_count)

        store.log_event("backfill_checkpoint", "backfill_job", "backfill_job", {
            "task_id": entry_id,
            "checkpoint": processed_count,
        })

        return entry, f"回填任务检查点已更新：已处理 {processed_count} 条记录"

    def complete_backfill(self, entry_id: int, total_records: int) -> tuple[dict[str, Any] | None, str]:
        """完成回填任务，通知治理报告。

        已提交分段的范围是最终统计口径：存在分段记录时以已提交范围为准，
        已提交数据不会被重复计算。检查点和分段记录在任务关闭后保留，
        供分段详情继续对照各段字段定义及行数。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可完成"

        table_name = entry.get("目标湖表", "")

        # 以已提交分段范围为准，已提交数据不重复统计
        segments = self._segments.get(entry_id, [])
        if segments:
            total_records = segments[-1]["range_end"]

        entry["status"] = "已完成"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["处理记录数"] = str(total_records)

        # 通知治理报告
        store.log_event("backfill_completed_to_govern", "backfill_job", "govern_report", {
            "task_id": entry_id,
            "table_name": table_name,
            "total_records": total_records,
        })

        return entry, f"回填任务 {entry.get('任务名称')} 已完成，总计 {total_records} 条记录"

    def fail_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """回填失败：保留检查点以便恢复。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, "回填任务不存在"
        if entry.get("status") != "回填中":
            return None, f"回填任务状态为{entry.get('status')}，不可标记失败"

        checkpoint = self._checkpoints.get(entry_id, 0)

        entry["status"] = "回填失败"
        entry["pending"] = False
        entry["abnormal"] = True

        store.log_event("backfill_failed", "backfill_job", "retention_mgr", {
            "task_id": entry_id,
            "failed_at_checkpoint": checkpoint,
        })

        return entry, f"回填任务失败，检查点保留在 {checkpoint} 条，可从该位置恢复"

    def get_checkpoint(self, entry_id: int) -> int | None:
        """获取回填检查点。"""
        return self._checkpoints.get(entry_id)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "启动回填":
            return self.start_backfill(entry_id)
        if action == "暂停回填":
            return self.pause_backfill(entry_id)
        if action == "恢复回填":
            return self.resume_backfill(entry_id)
        return super().run_action(entry_id, action)
