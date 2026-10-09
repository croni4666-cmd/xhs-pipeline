# 🗺️ xhs-pipeline 工程路线图 (Engineering Roadmap)

> **项目愿景**：打造工业级、开放式、严格区分“内容实体/采集上下文/任务状态”的小红书（Xiaohongshu）AI Agent 自动化研究与知识资产化管线。  
> **核心原则**：契约上下文解耦（NoteReference）、学术研究可复现（CrawlTaskManifest）、崩溃一致性原子存储（Atomic Storage）、Taleb 式弹性自治防风控（Anti-Ruin Resilience）。

---

## 🎯 阶段演进总览 (Milestones & Engineering Status)

```
[Phase 1: 稳定最小闭环]  ✅ 已完成 (v0.1.0)
 └─ 标准: 数据校验、安装验证、幂等导出与基础契约测试通过
         │
         ▼
[Phase 2: 验证驱动替换]  ✅ 已完成 (v0.2.0)
 └─ 标准: 接入异构 HttpCrawlerDriver，下游清洗/分析/导出代码零修改
         │
         ▼
[Phase 3: 实体/上下文解耦与可复现研究]  ✅ 已完成 (v0.3.0)
 └─ 标准: NoteReference 访问上下文隔离、DiscoveryRecord 独立溯源、
         FieldPresence 显式缺失语义、CrawlTaskManifest 检索条件沉淀、
         原子文件写入与崩溃一致性对账
         │
         ▼
[Phase 4: 持续运行与弹性自治]  ✅ 已完成 (v0.3.0)
 └─ 标准: 请求配额预算、指数退避重试、断路器熔断保护、驱动自动故障转移
```

---

## 📌 Phase 3: 实体、上下文与任务状态深度分离 (v0.3.0) — ✅ 已完成

**目标**：解决“笔记实体”、“采集上下文”与“任务执行状态”耦合问题，实现高置信度研究复现与崩溃一致性。

- [x] **访问上下文与实体彻底解耦 (`NoteReference`)**：
  - 搜索与信息流阶段仅产出轻量级引用 `NoteReference`，私有封装 `access_token`（如 `xsec_token`）、请求头及会话参数，绝不污染知识库正文实体。
  - 明确失效语义：引入 `ReferenceExpiredError`，支持声明跨进程恢复能力（`can_resume_cross_process`），杜绝无效令牌无限重试。
- [x] **独立发现轨迹与多词去重 (`DiscoveryRecord` & `DataCleaner`)**：
  - 彻底将 `source_keyword` 从笔记核心实体中剔除；直接通过 `note_id` 查询详情无需任何虚构关键词。
  - 同一笔记命中多关键词或主页链接时，`DataCleaner` 自动合并发现路径，确保内容实体全局唯一且完整保留所有研究溯源。
- [x] **显式字段完整性语义 (`FieldPresence`)**：
  - 引入 `FieldPresence`（`VALID`, `NOT_FETCHED`, `FETCH_FAILED`, `UNSUPPORTED`, `KNOWN_EMPTY`）。
  - 坚决杜绝隐性失真：未采集评论数不再填为 `"0"`，未抓取正文不再填为空白，图片未请求与确认无图明确区分。
- [x] **研究复现任务清单 (`CrawlTaskManifest`)**：
  - 记录完整检索参数（关键词、排序模式、过滤规则、时间戳）、配额上限、实际发现数、去重数、详情成功率及内容版本哈希映射。
  - 支持导出与保存 `manifest_{task_id}.json`，彻底消除学术研究中的样本选择偏差。
- [x] **原子写入与崩溃一致性对账 (`ObsidianExporter` & `CheckpointStore`)**：
  - 采用临时文件写入、刷盘与原子重命名机制（`.tmp` -> `os.replace`），杜绝进程崩溃产生半写损坏文件。
  - 崩溃恢复对账：基于 SHA-256 内容指纹对账，若文件已落盘且指纹吻合，自动提交检查点，绝不产生重复卡片与漏写。
- [x] **严格协程取消语义与子进程生命周期治理**：
  - `MediaCrawlerDriver` 协程内嵌取消监听，收到 `asyncio.CancelledError` 时安全终结外部爬虫子进程并重新抛出异常，杜绝后台僵尸进程。
- [x] **Pipeline 职责拆分与组件解耦**：
  - `XhsPipeline` 剥离为纯净的流程与状态协调器；
  - 独立出数据清洗组件 [`core/normalizer.py`](core/normalizer.py)（`DataCleaner`）；
  - 独立出资产持久化组件 [`core/storage.py`](core/storage.py)（`ObsidianExporter`, `CsvExporter`）；
  - 独立出状态机与对账组件 [`core/checkpoint.py`](core/checkpoint.py)（`CheckpointStore`）。

---

## 🛡️ 可观测、可暂停、可恢复的运行策略 (Anti-Ruin Operational Strategy)

| 场景 | 平台表现 | 运行策略与处理方式 |
| :--- | :--- | :--- |
| **令牌失效 (Token Expired)** | HTTP 401 / xsec_token 过期 | 抛出 `ReferenceExpiredError(needs_re_search=True)`，终止针对该令牌的无意义重试，提示重新检索。 |
| **限流响应 (Rate Limit)** | HTTP 429 / 提示访问过于频繁 | 触发 `RateLimitError`，激活 `ExponentialBackoff` 指数退避重试；连续失败触发断路器跳闸。 |
| **任务主动取消 (Cancelled)** | Agent 或用户中断协程 | 捕获 `asyncio.CancelledError`，执行资源清理并终止外部子进程，重新传播取消异常，杜绝误判为失败重试。 |
| **崩溃后重启 (Post-Crash)** | 进程异常退出后重新拉起 | `CheckpointStore` 对账磁盘既有文件与 SHA-256 指纹，无损恢复状态，不产生重复文件。 |
