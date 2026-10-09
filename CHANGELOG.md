# 📝 变更日志 (CHANGELOG)

本项目遵循 [语义化版本 2.0.0 (Semantic Versioning)](https://semver.org/lang/zh-CN/) 规范。  
变更日志格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)。

## [0.3.2rc3] - 2026-10-09

### 🛡️ 审查闭环加固与完整性保证 (Comprehensive Audit Closure & Integrity Hardening)

系统性响应 GPT 第二轮复审报告（F01–F07 残留项），彻底消除所有 P1 级安全与数据一致性隐患，完善 P2 级健壮性契约：

#### 🔒 Security & Credential Boundary (安全注入封堵与凭证完全隔离)
- **F01 终结（多轮配置注入彻底封堵）**：
  - MediaCrawler `base_config.py` 改用行首至行尾的多行模式全行替换（`(?m)^[ \t]*KEYWORDS\s*=.*$`）配合 `lambda` 纯字面量返回，彻底解决由于字符串转义引号导致的早停与 Python 语句注入逃逸问题。
  - 新增多轮注入复查探针，经 AST 深度验证无任何变量名外溢。
- **F02 终结（凭证完全隔离与敏感数据脱敏）**：
  - 零凭证落盘原则：引用持久化（`save_references`）全面剔除敏感令牌及会话凭据（自动过滤 `cookie`, `token`, `auth`, `secret`, `key` 等敏感字段，`access_token` 不再写入持久化 JSON）。
  - 图片列表（`image_list`）与视频链接（`video_url`）全面应用 `sanitize_note_url` 净化，消除媒体 URL 泄露 `xsec_token` 的隐蔽路径。
  - 错误信息脱敏（`_sanitize_error_msg`）全面支持大小写不敏感匹配、JSON 键值对格式与等号/冒号 Cookie 捕获。

#### 📦 Data Authenticity & Contract Integrity (数据真实性与生产门禁)
- **F03 终结（模拟模式显式标注与回退门禁）**：
  - `config/settings.example.json` 与默认配置模板中 `fallback_driver` 彻底改为 `null`，生产环境严格禁止隐式/无感知回退到 mock。
  - `HttpCrawlerDriver` 显式标注 `is_synthetic = True` 与 `data_source = "synthetic_simulation"`，搜索与详情均透明标识为模拟生成，不再以真实采集数据自称。
  - 发现记录明确保留搜索排名 `rank` 并在 Manifest 中完整持久化。

#### 🔄 Idempotence, Annotations & Reconciliation (幂等对账与人工批注强保护)
- **F04 终结（卡片篡改自愈与全量输入渲染指纹）**：
  - `reconcile_after_crash` 强制校验已提交文件的磁盘二进制 SHA-256 哈希值；若卡片被外部破坏、截断或篡改，绝不再误判为 `EXPORTED`，自动触发重新导出并修复。
  - 引入全量渲染输入哈希（`get_render_input_hash`），囊括点赞指标变化、发现关键词追加、分析洞察更新等所有可渲染元素；指标或结论变化时准确触发重写，不再被 `source_hash` 错误跳过。
  - 无提交日志的崩溃卡片恢复强化验证：严格校验 Frontmatter `note_id`、`content_hash`、文章一级标题与正文区块位置，拒绝伪造或格式不全的骨架卡片。
- **F05 终结（人工批注绝对保护与文件名歧义消除）**：
  - 引入结构化保护注释 `<!-- BEGIN_USER_NOTES -->` 与 `<!-- END_USER_NOTES -->`，即使用户在人工批注区再次引用同名 Markdown 标题，全文批注均完整无损保留。
  - 旧卡片兼容与保护：存在但无指定标头的已有卡片内容自动沉淀至人工批注区，绝不静默覆盖。
  - 卡片文件查找严格比对 Frontmatter `note_id: "{note_id}"`，彻底消除包含下划线的笔记 ID（如 `abc` 与 `abc_def`）之间的前缀路径冲突。

#### 🚀 CLI Contract & Resilience (CLI 失败状态码一致性与熔断持久化)
- **F06 优化（并发元数据保护）**：
  - Checkpoint 提交日志全面采用安全隔离与异常捕获，损坏文件自动归档至 `.corrupt_*` 并不中断服务。
- **F07 终结（CLI 退出契约一致性）**：
  - CLI `main` 与 JSON 输出严格绑定 `CrawlResponse.success`；采集失败即使存在部分脏数据，或无诊断信息返回，均稳定退出非零状态码（exit 2）并输出 `"success": false`。
  - 跨任务熔断状态持久化：`XhsPipeline` 维护长生命周期 `router`，确保熔断器状态在多次任务调用间保持连续有效。

---

## [0.3.1rc2] - 2026-10-09

### 🛡️ 架构与安全全面加固 (Rigorous Hardening & Security Isolation)

系统性修复预发布工程审查报告（F01-F07）发现的所有阻塞项：

#### 🔒 Security & Credential Isolation (安全与凭证隔离)
- **F01 修复（防止配置注入）**：MediaCrawler 配置写入全面使用安全字面量编码（`json.dumps`），并强制通过 `ast.parse` 抽象语法树校验，杜绝恶意字符串逃逸为可执行代码。
- **F02 修复（凭证边界封堵）**：引入统一 URL 规范化函数 `sanitize_note_url`，去除所有 `xsec_token` 等查询参数；非跨进程恢复凭证禁止保存至持久化 JSON；错误输出（stderr）全面实施敏感路径与 Token 脱敏。
- **CSV 公式注入防御**：导出表格时对以 `=`, `+`, `-`, `@` 起始的文本字段自动转义，防止电子表格软件解析为可执行宏公式。

#### 📦 Architecture & Reliability (架构完备性与真实性保证)
- **F03 修复（驱动能力声明与真实保证）**：修正 `MediaCrawlerDriver` 详情能力声明（`can_get_detail = False`）；`HttpCrawlerDriver` 采用确定性 SHA-256 派生 ID；生产环境默认关闭自动向 mock 回退。
- **F04 修复（对账恢复完整性）**：重构 `reconcile_after_crash`，强制核查磁盘文件真实存在性、正文完整结构与内容指纹；统一 Windows 平台 LF 字节写入与完整 64 位 SHA-256 渲染对账；将对账逻辑贯穿至正常导出流程。
- **F05 修复（人工批注强保护）**：严格精确匹配 `XHS_{note_id}.md` 文件，彻底杜绝短 ID 前缀碰撞；采用尾部反向分割（`rsplit`）提取人工批注；现有卡片读取失败时阻断覆盖，确保数据不丢失。
- **F06 修复（元数据原子化写入）**：任务清单、引用列表与提交日志全面采用基于 PID 与纳秒时间戳的唯一临时文件原子写入（`flush + fsync + os.replace`），彻底消除写入中断致空文件截断隐患。
- **F07 修复（发布包与 CLI 入口）**：显式配置 `tool.setuptools.packages.find` 支持平铺布局构建 Wheel；提供同步包装入口 `cli_entrypoint` 解决 CLI 报错；实现 `save_state`；采集失败依据结果退出码返回非零状态。

---

## [0.3.0] - 2026-10-08

### 🔬 实体、上下文与任务状态深度分离 (Deep Separation of Entities, Context & State)

本次版本系统性响应第二轮专家审查意见，消除了“笔记实体”、“采集上下文”与“任务执行状态”之间的隐性耦合，全面奠定学术可复现性与工业级容灾一致性基础。

#### ✨ Added (新增功能)
- **访问上下文与实体彻底解耦 (`NoteReference`)**：
  - 搜索与信息流阶段仅产出轻量级引用 `NoteReference`，私有封装 `access_token`（如 `xsec_token`）、请求头及会话参数，绝不污染知识库正文实体。
  - 明确失效语义：引入 `ReferenceExpiredError`，支持声明跨进程恢复能力（`can_resume_cross_process`），杜绝无效令牌无限重试。
- **独立发现轨迹与多词去重 (`DiscoveryRecord` & `DataCleaner`)**：
  - 彻底将 `source_keyword` 从笔记核心实体中剔除；直接通过 `note_id` 查询详情无需任何虚构关键词。
  - 同一笔记命中多关键词或主页链接时，`DataCleaner` 自动合并发现路径，确保内容实体全局唯一且完整保留所有研究溯源。
- **显式字段完整性语义 (`FieldPresence`)**：
  - 引入 `FieldPresence`（`VALID`, `NOT_FETCHED`, `FETCH_FAILED`, `UNSUPPORTED`, `KNOWN_EMPTY`）。
  - 坚决杜绝隐性失真：未采集评论数不再填为 `"0"`，未抓取正文不再填为空白，图片未请求与确认无图明确区分。
- **研究复现任务清单 (`CrawlTaskManifest`)**：
  - 记录完整检索参数（关键词、排序模式、过滤规则、时间戳）、配额上限、实际发现数、去重数、详情成功率及内容版本哈希映射。
  - 支持导出与保存 `manifest_{task_id}.json`，彻底消除学术研究中的样本选择偏差。
- **原子写入与崩溃一致性对账 (`ObsidianExporter` & `CheckpointStore`)**：
  - 采用临时文件写入、刷盘与原子重命名机制（`.tmp` -> `os.replace`），杜绝进程崩溃产生半写损坏文件。
  - 崩溃恢复对账：基于 SHA-256 内容指纹对账，若文件已落盘且指纹吻合，自动提交检查点，绝不产生重复卡片与漏写。
- **严格协程取消语义与子进程生命周期治理**：
  - `MediaCrawlerDriver` 协程内嵌取消监听，收到 `asyncio.CancelledError` 时安全终结外部爬虫子进程并重新抛出异常，杜绝后台僵尸进程。

#### 🔄 Changed (优化变更)
- **Pipeline 职责拆分与组件解耦**：
  - `XhsPipeline` 剥离为纯净的流程与状态协调器；
  - 独立出数据清洗组件 [`core/normalizer.py`](core/normalizer.py)（`DataCleaner`）；
  - 独立出资产持久化组件 [`core/storage.py`](core/storage.py)（`ObsidianExporter`, `CsvExporter`）；
  - 独立出状态机与对账组件 [`core/checkpoint.py`](core/checkpoint.py)（`CheckpointStore`）。

---

## [0.2.0] - 2026-10-07

### 🚀 架构重构与能力升级 (Milestone Implementation: Phase 1–4 Complete)

- 实装第二种异构驱动 `HttpCrawlerDriver`，验证可替换性。
- 解耦 `ContentFact`, `EngagementSnapshot`, `ContentInsight` 溯源建模。
- 落地 `core/resilience.py` 请求预算配额、指数退避、断路器熔断与故障转移。
- 引入通用驱动一致性测试套件 `tests/test_conformance.py`。

---

## [0.1.0] - 2026-10-07

### 🌟 初始发布 (Initial Release)

- 确立 `BaseCrawlerDriver` 与 `MediaCrawlerDriver` (CDP 真实浏览器) 基础。
- 统一实体 `UnifiedNote` 与 `UnifiedComment`。
- Obsidian 双链知识卡片导出与基础爆款模式分析。
