# 📕 xhs-pipeline (v0.3.2-rc3)

> **工业级、开放式、严格区分“内容实体/采集上下文/任务状态”的小红书（Xiaohongshu）AI Agent 自动化研究与知识资产化管线。**  
> *Industrial-grade Xiaohongshu Research, Context-Decoupled Multi-Driver Architecture, Academic Reproducibility & Obsidian PKM Pipeline.*

[![Version](https://img.shields.io/badge/version-0.3.2rc3-blue.svg)](VERSION)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](pyproject.toml)
[![Architecture](https://img.shields.io/badge/architecture-Context%20Decoupled-orange)](ARCHITECTURE.md)
[![Tests](https://img.shields.io/badge/tests-22%20passing-brightgreen)](tests/)
[![Security](https://img.shields.io/badge/security-hardened-green)](tests/test_audit_probes.py)

---

## 📌 项目完整目录清单 (Project Directory Manifest)

本项目采用现代高内聚、低耦合的模块化设计，清晰解耦底层采集、上层调度、弹性风控治理与知识资产化沉淀：

```
xhs-pipeline/
├── VERSION                         # [版本声明] 单一真实来源的语义化版本标识 (0.3.2rc3)
├── CHANGELOG.md                    # [变更日志] 遵循 Keep a Changelog 规范的演进历史
├── LICENSE                         # [开源许可] MIT 许可证
├── pyproject.toml                  # [打包配置] 遵循 PEP 621 标准的 Python 项目打包与 CLI 声明
├── README.md                       # [项目手册] 开发者与审查者总览文档
├── SKILL.md                        # [Agent 协议] Antigravity 官方 Skill 触发词与 Prompt 规约
├── ROADMAP.md                      # [演进路线] 实体/上下文解耦与可复现研究规划
├── ARCHITECTURE.md                 # [架构白皮书] 系统分层架构、时序图、崩溃对账与契约规约
├── config/
│   └── settings.json               # [系统配置] 默认驱动、备用驱动、请求配额与知识库路径配置
├── core/
│   ├── __init__.py                 # [核心导出] 统一导出 __version__、实体模型及核心组件
│   ├── contracts.py                # [数据契约] ContentFact / NoteReference / FieldPresence / CrawlTaskManifest
│   ├── normalizer.py               # [清洗去重] DataCleaner (多关键词发现轨迹合并、去重、防注入)
│   ├── storage.py                  # [资产导出] ObsidianExporter (原子写入、用户批注持久化) & CsvExporter
│   ├── checkpoint.py               # [状态对账] CheckpointStore (崩溃指纹对账、跨进程引用持久化)
│   ├── resilience.py               # [弹性风控] 请求预算、指数退避、断路器熔断与故障转移路由
│   ├── pipeline.py                 # [核心调度] XhsPipeline (轻量流程协调器，依赖注入)
│   └── drivers/                    # [驱动抽象] 可插拔爬虫驱动层
│       ├── __init__.py             # [驱动导出] 统一驱动导出
│       ├── base.py                 # [驱动基类] BaseCrawlerDriver 抽象生命周期与接口规约
│       ├── mediacrawler.py         # [默认驱动] 基于 CDP 9222 端口真实 Edge 浏览器挂载驱动
│       ├── http_driver.py          # [异构驱动] 纯异步 HTTP 驱动 (脱离浏览器, 验证可替换性)
│       └── mock_driver.py          # [测试驱动] 离线环境与 CI 契约测试模拟驱动
├── scripts/
│   ├── run_pipeline.py             # [CLI 工具] 统一命令行采集入口（支持 --format json 与故障转移）
│   ├── export_obsidian.py          # [卡片转换] JSONL 转 Obsidian Markdown（非破坏性保留用户批注）
│   └── analyze_insights.py         # [模式分析] 可追溯洞察生成器（带原文证据引用与版本指纹）
└── tests/
    ├── test_pipeline.py            # [管线测试] 9 项完整单元与验收测试（包含对账、去重、复现清单）
    └── test_conformance.py         # [契约测试] 通用驱动一致性测试套件（验证引用失效与取消语义）
```

---

## 🏛️ 第二轮审查意见系统性修订答辩 (Audit Items Response)

针对专家审查提出的“笔记实体”、“采集上下文”与“任务执行状态”未充分分离的问题，本版本落地了彻底的架构重构：

### 1. 搜索结果与详情查询的上下文解耦 (`NoteReference`)
- **修订方案**：引入 `NoteReference` 承载驱动特定的访问上下文（如 `xsec_token`、Session 参数）。
- **边界保证**：`NoteReference` 仅在驱动间流转，绝不进入知识卡片正文或业务实体。
- **失效语义**：令牌失效时抛出明确的 `ReferenceExpiredError(needs_re_search=True)`，终止无意义重试。
- **跨进程恢复**：`CheckpointStore` 支持跨进程保存与校验引用有效性。

### 2. 彻底移出 `source_keyword`，独立记录发现来源
- **修订方案**：`UnifiedNote` 与 `ContentFact` 彻底剥离单一 `source_keyword` 字段。
- **直查零虚构**：直接通过 `note_id` 查询单篇笔记无需任何虚构关键词（`discovery_records` 为空或记录直查来源）。
- **多词去重与轨迹合并**：同一笔记命中多个关键词时，`DataCleaner` 自动合并 `DiscoveryRecord` 列表，内容实体全局唯一且保留所有研究路径。

### 3. 显式字段状态语义，杜绝隐性假数据 (`FieldPresence`)
- **修订方案**：定义 `FieldPresence` 枚举（`VALID`, `NOT_FETCHED`, `FETCH_FAILED`, `UNSUPPORTED`, `KNOWN_EMPTY`）。
- **杜绝失真**：未采集评论数不再填为 `"0"`，而是标记为 `FieldPresence.NOT_FETCHED`；未请求图片与确认无图严格区分。

### 4. 协程取消语义与子进程生命周期治理
- **修订方案**：`MediaCrawlerDriver` 协程全面内嵌 `asyncio.CancelledError` 捕获。
- **无僵尸进程**：当用户或 Agent 中断任务时，自动安全终结（`terminate()`/`kill()`）后台爬虫子进程并释放调试端口句柄，正确重新抛出取消异常。

### 5. 原子持久化与崩溃一致性对账 (Crash Consistency)
- **修订方案**：`ObsidianExporter` 采用写入唯一临时文件并刷盘原子重命名机制（`.tmp` -> `os.replace`），消除并发与写入中断损坏风险。
- **崩溃对账**：`CheckpointStore` 在重启后根据卡片中的 SHA-256 `content_hash` 指纹与正文完整结构进行对账。文件已存在且哈希吻合时确认检查点，防止重复覆盖并严格保留用户人工批注。

### 6. 研究复现任务清单 (`CrawlTaskManifest`)
- **修订方案**：引入 `CrawlTaskManifest`，不仅保存笔记，更保存完整检索条件（关键词、排序、过滤规则）、配额上限、实际发现数、去重数、详情成功率及内容版本哈希字典。
- **消除选择偏差**：完整记录“为什么选中了这些内容”，为实证研究与文献计量提供透明、可追溯的审计追踪。

### 7. 安全防御与凭证边界封堵
- **配置防注入**：配置参数强制经由安全字面量编码与 AST 抽象语法树校验，杜绝注入。
- **URL 与凭证隔离**：全链路规范化清除 URL 中的敏感令牌；非跨进程凭证不落盘。
- **CSV 公式转义**：表格导出对特殊公式前缀（`=`, `+`, `-`, `@`）自动转义。

### 8. Pipeline 职责拆解，防止单体膨胀
- **修订方案**：`XhsPipeline` 剥离为轻量级流程与状态协调器，具体逻辑委托给独立组件：
  - 数据清洗与去重：`DataCleaner`
  - 资产持久化与原子写入：`ObsidianExporter` / `CsvExporter`
  - 检查点与崩溃对账：`CheckpointStore`
  - 弹性风控与故障转移：`core/resilience.py`

---

## ⚡ 快速使用指南 (Quick Start)

### 1. 运行自检与合规测试
```bash
# 运行全部 19 项单元测试、契约合规与审计探针测试
pytest -v
```

### 2. 执行关键词采集并导出复现清单与 Obsidian 卡片
```bash
python scripts/run_pipeline.py \
  --keywords "杭州美食,杭州旅游攻略" \
  --limit 20 \
  --driver mediacrawler \
  --fallback-driver mock \
  --max-budget 50 \
  --analyze \
  --export-obsidian "./obsidian_cards" \
  --export-csv "data/summary.csv"
```

### 3. 机器可读 JSON 输出 (供 AI Agent / 自动化工具直接解析)
```bash
python scripts/run_pipeline.py \
  --keywords "西湖" \
  --limit 5 \
  --driver mock \
  --format json
```

---

## 📄 许可证 (License)

本项目采用 [MIT 许可证](LICENSE)。
