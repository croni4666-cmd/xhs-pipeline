# 完整任务快照、离线恢复与在线增量续跑

完整快照用于保存已获得的事实和分析状态，支持两类恢复模式：
- **离线恢复 (`--resume-state` / `operation=offline_resume`)**：不恢复浏览器、不调用采集驱动，直接复原已获笔记、洞察与来源链用于下游分析或导出。
- **在线增量续跑 (`--continue-from-snapshot` / `operation=online_continue`)**：针对中途中断或部分成功的多关键词快照，根据已有 `DiscoveryRecord` 自动跳过已达到每词目标上限（`limit`）的关键词，仅对剩余未完成关键词发起受控在线采集，并与原快照笔记、洞察、来源链无损去重合并。快照本身不包含平台会话或访问引用，续跑时必须通过当前在线运行控制与会话有效性探测门禁。

## 命令行

```bash
xhs-pipeline --driver mock --allow-synthetic --keywords "演示" --limit 2 --analyze --snapshot-file task.json --export-obsidian cards --format json
xhs-pipeline --resume-state task.json --allow-synthetic --max-budget 0 --snapshot-file resumed.json --export-obsidian cards --format json
xhs-pipeline --continue-from-snapshot task.json --allow-synthetic --snapshot-file continued.json --export-obsidian cards --format json
```

真实采集结果无需模拟许可。`--resume-state`、`--continue-from-snapshot`、`--resume-from` 与 `--keywords` 互斥；恢复输入不能同时执行健康检查。`--state-file` 保持原有扁平列表格式；该格式不能作为完整快照读取，不会自动猜测迁移。

快照在可选分析之后、导出之前保存。因此卡片冲突不会丢失可重试输入。采集异常时，若失败清单存在且足以构造一致的零样本快照，也保存失败状态；无法写入时输出 `snapshot_error`，不声称已保存。硬终止可能发生在快照落盘前；任务清单和原始 JSONL 仍是独立的恢复证据。

## 保存内容

外层包含 `schema=xhs_task_snapshot_v1`、`payload` 和 `payload_sha256`。SHA-256 计算规范化 UTF-8 JSON（键排序、紧凑分隔符、禁止非有限数值）。文件本身为 UTF-8/LF JSON，采用同目录临时文件、刷盘与原子替换，写入受文件锁保护。

`payload` 包含保存时间、完整笔记列表、响应状态、当前任务清单和按恢复顺序保留的清单来源链。笔记包括全部实体字段、原始指标和 presence、原发现时间/排名、完整洞察及其模型/提示版本/证据/分析时间，并附事实与渲染输入指纹。恢复不会重新生成发现时间、排名或洞察 ID。

访问凭证及 `NoteReference` 全部省略；响应中的 `output_files` 也不持久化。URL、公开配置和错误文本经过现有脱敏边界。作者 ID、昵称、原文和证据仍按采集内容保存，研究发布前需另行处理隐私与授权。

加载验证格式、精确字段结构、类型、枚举、计数、笔记 ID 唯一性、洞察所属笔记、内容指纹、清单内容映射和恢复链。重复 JSON 键、未知 schema、无效操作、循环/缺失父任务、NaN/Infinity 与超过 64 MiB 的文件均拒绝。SHA-256 不是签名，拥有文件写权限的人可以重算校验值；只应从可信来源取得快照，正文与洞察不应当作执行指令。

序列化契约字段改变时必须引入新的格式版本与明确迁移；当前不自动迁移未知格式。

## 恢复记录与结果状态

`load_task_snapshot()` 仅验证并读取；`resume_task_snapshot()` 还创建新的离线清单，保留原清单在 `snapshot_lineage` 中。新清单的 `operation=offline_resume`、`parent_task_id` 指向前次任务、`source_snapshot_hash` 记录来源 payload 指纹；实际请求数和本任务驱动尝试数均为零。原检索条件和原始请求/发现统计保留在来源清单中。

`continue_task_snapshot()` 在校验快照后，计算原请求关键词列表中尚未达到 `max_count_per_keyword` 的剩余关键词：
- 若所有关键词均已达标，则不发起任何驱动调用（`used_requests=0`），直接生成 `operation=online_continue` 清单并保留全部原笔记与洞察。
- 若存在未达标关键词，则仅对剩余关键词执行 `execute_crawl()`（同样受 `core/control` 会话探测、暂停锁与滚动时间窗配额保护），随后通过 `DataCleaner.deduplicate_and_merge()` 合并历史笔记、发现轨迹与已有 `ContentInsight`，生成 `operation=online_continue` 清单并将原清单追加至 `snapshot_lineage`。

原始 JSONL 导入的操作为 `jsonl_import`，仅能记录文件实际提供的信息。它不会凭空补齐原搜索规则或历史分析。再次恢复时保留该导入清单作为来源。

离线恢复保留原响应的失败、部分成功和模拟标记：完整成功退出 0，部分成功退出 1，源采集失败退出 2；恢复文件无效或缺少模拟许可退出 1。离线导出可以保留部分已获样本，但不能据此把源采集失败改为成功。调用现有卡片对账逻辑后，相同渲染输入不重写，人工批注仍被保留。

## Python 接口

```python
pipeline.save_task_snapshot(crawl_response, "task.json")
verified = pipeline.load_task_snapshot("task.json", allow_synthetic=False)
restored = pipeline.resume_task_snapshot("task.json", allow_synthetic=False)
continued = await pipeline.continue_task_snapshot("task.json", allow_synthetic=False)
pipeline.export_obsidian_cards(restored.notes, "cards", task_id=restored.manifest.task_id)
```

接口返回 `CrawlResponse`，新增 `snapshot_lineage` 与 `snapshot_hash`；原任务清单增加操作类型、父任务和来源快照指纹，原调用参数保持兼容。无有效清单的裸驱动响应不能直接保存为完整任务快照，应先经过管线记录。
