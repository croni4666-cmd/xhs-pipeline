---
name: xhs-pipeline
description: 小红书（Xiaohongshu）全流程智能采集、内容模式分析与 Obsidian 知识资产化管线。基于驱动契约架构（Driver Contract），实现笔记实体、访问上下文与任务状态深度分离；支持 MediaCrawler(CDP) 与纯异步 Http 异构驱动无缝切换；具备请求配额、指数退避与断路器防风控弹性治理；支持长文正文抓取、学术级可复现任务清单导出、原子文件写入与非破坏性 Obsidian 知识卡片沉淀。
triggers:
  - 小红书
  - 小红书抓取
  - 小红书调研
  - 小红书爆款
  - 小红书选题
  - 小红书分析
  - 小红书转obsidian
  - 小红书正文
  - xhs
  - xhs pipeline
  - mediacrawler
  - xiaohongshu skill
---

# 📕 xhs-pipeline: 小红书智能研究与资产化管线 (v0.3.0)

遵循**工业级软件工程标准**与**学术可复现性规范**构建的小红书自动化数据与知识沉淀 Skill。

```
[用户/Agent 提出研究需求]
          │
          ▼
[弹性治理层 (Resilience)] ──> 请求预算配额 / 指数退避 / 断路器熔断 / 故障转移
          │
          ▼
[xhs-pipeline 调度管线] ──> [领域模型 (ContentFact / NoteReference / FieldPresence)]
          │
    ┌─────┴───────────────────────────┐
    ▼                                 ▼
[驱动契约抽象层 (Driver Layer)]     [核心处理与资产持久化层]
    │                                 │
    ├─ MediaCrawler (CDP 真实浏览器)   ├─ DataCleaner (多词发现轨迹去重合并)
    ├─ HttpCrawlerDriver (纯异步 HTTP) ├─ ObsidianExporter (原子写入, 保护用户批注)
    └─ MockDriver (离线/CI 契约测试)   ├─ CheckpointStore (崩溃对账与复现清单)
                                      └─ 可追溯模式分析 (证据段落引用与版本指纹)
```

---

## 🚀 快速启动指南 (Quick Start)

### 1. 确保浏览器调试环境就绪 (若使用 CDP 驱动)
如果尚未启动 CDP 调试浏览器，运行项目配置的批处理脚本：
```powershell
# 启动带 9222 调试端口的浏览器
.\start_edge_debug.bat
```
*(已登录过的用户无需再次登录，用户配置文件自动持久化)*

### 2. 检查驱动健康状态
```powershell
# 检查默认 CDP 驱动
python scripts/run_pipeline.py --driver mediacrawler --health-check

# 检查纯 HTTP 异构驱动
python scripts/run_pipeline.py --driver http --health-check
```

### 3. 一键抓取、可追溯分析并导出到 Obsidian
```powershell
python scripts/run_pipeline.py `
  --keywords "杭州美食,杭州旅游攻略" `
  --limit 20 `
  --driver mediacrawler `
  --fallback-driver mock `
  --max-budget 50 `
  --analyze `
  --export-obsidian "./obsidian_cards" `
  --export-csv "./data/summary.csv"
```

### 4. 离线断点恢复 (零网络开销复用已有数据)
```powershell
python scripts/run_pipeline.py `
  --resume-from "./data/xhs/jsonl/search_contents.jsonl" `
  --export-obsidian "./obsidian_cards"
```

### 5. 样本内容模式与特征分析 (可追溯证据引用)
```powershell
python scripts/analyze_insights.py `
  --input "./data/xhs/jsonl/search_contents.jsonl" `
  --format json
```

---

## 🛠️ 工程化核心文件索引

| 模块 / 规范文件 | 工程定位与职责说明 |
| :--- | :--- |
| [`VERSION`](VERSION) | 单一真实来源的语义化版本号标识 (`0.3.0`) |
| [`CHANGELOG.md`](CHANGELOG.md) | 遵循 Keep a Changelog 规范的详细变更履历与升级说明 |
| [`LICENSE`](LICENSE) | MIT 开源许可证 |
| [`pyproject.toml`](pyproject.toml) | 遵循 PEP 621 标准的现代化包构建元数据与 CLI 声明 |
| [`ROADMAP.md`](ROADMAP.md) | 实体/上下文解耦与可复现研究规划路线图 |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | 系统分层架构、实体时序图、崩溃对账与契约规约 |
| [`core/contracts.py`](core/contracts.py) | `ContentFact`、`NoteReference`、`FieldPresence`、`CrawlTaskManifest` |
| [`core/normalizer.py`](core/normalizer.py) | 数据清洗组件 `DataCleaner`（多词轨迹合并去重、正文完整性校验） |
| [`core/storage.py`](core/storage.py) | 持久化导出组件 `ObsidianExporter`（原子写入、保护用户批注）与 `CsvExporter` |
| [`core/checkpoint.py`](core/checkpoint.py) | 状态对账组件 `CheckpointStore`（崩溃指纹对账、跨进程引用持久化） |
| [`core/resilience.py`](core/resilience.py) | 请求配额预算、指数退避重试、断路器熔断与驱动故障转移路由 |
| [`core/pipeline.py`](core/pipeline.py) | 核心调度管线（纯净流程协调器，基于依赖注入） |
| [`tests/test_conformance.py`](tests/test_conformance.py) | 通用驱动一致性合规测试套件（验证引用失效与取消语义） |
| [`tests/test_pipeline.py`](tests/test_pipeline.py) | 9 项全流程单元与验收测试（多词去重、直查零虚构、崩溃对账等） |
