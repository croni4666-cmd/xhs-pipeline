---
name: xhs-pipeline
description: 小红书采集调度、数据来源记录和 Obsidian 导出管线。真实采集依赖外部 MediaCrawler；http/mock 仅提供显式许可的模拟数据。支持驱动调用预算、任务清单、字段状态、原子导出与人工批注保护。
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

# 📕 xhs-pipeline: 小红书智能研究与资产化管线 (v0.4.0-rc1)

预发布采集调度与知识沉淀 Skill。使用前遵守 README 和 SECURITY.md 中的当前能力边界，不将模拟数据作为真实样本，不自动发布采集结果或访问凭证。

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
本仓库不提供浏览器启动脚本。使用者应自行配置已授权的本地 CDP 浏览器与 MediaCrawler，并在本地 settings 中指定路径、解释器与端口。调试端口应仅在受控本地环境开放。

### 2. 检查驱动健康状态与在线运行控制状态
```powershell
# 检查默认 CDP 驱动
python scripts/run_pipeline.py --driver mediacrawler --health-check

# 查看本地会话范围暂停与受控请求配额状态（不发起网络请求）
python scripts/run_pipeline.py --control-status --format json

# 人工排查风险后解除本地暂停（保留 24h 请求账本，下次任务重新探测会话）
python scripts/run_pipeline.py --acknowledge-pause --format json

# 检查 HTTP 模拟驱动的本地初始化（不证明真实平台可达）
python scripts/run_pipeline.py --driver http --health-check
```

### 3. 一键抓取、可追溯分析并导出到 Obsidian
```powershell
python scripts/run_pipeline.py `
  --keywords "杭州美食,杭州旅游攻略" `
  --limit 20 `
  --driver mediacrawler `
  --max-budget 50 `
  --analyze `
  --export-obsidian "./obsidian_cards" `
  --export-csv "./data/summary.csv"
```

### 4. 离线断点恢复与在线增量续跑
完整任务使用统一 CLI 的 `--resume-state` 离线恢复 `--snapshot-file` 生成的快照；若多关键词任务因风控或配额中途中断并已人工确认解除暂停，可使用 `--continue-from-snapshot` 仅对未达标关键词继续在线采集并无损合并已有笔记、洞察与来源链；原始 JSONL 使用 `--resume-from`。模拟输入必须显式 `--allow-synthetic`。
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
| [`VERSION`](VERSION) | 单一真实来源的语义化版本号标识 (`0.4.0rc1`) |
| [`CHANGELOG.md`](CHANGELOG.md) | 遵循 Keep a Changelog 规范的详细变更履历与升级说明 |
| [`LICENSE`](LICENSE) | MIT 开源许可证 |
| [`pyproject.toml`](pyproject.toml) | 遵循 PEP 621 标准的现代化包构建元数据与 CLI 声明 |
| [`ROADMAP.md`](ROADMAP.md) | 实体/上下文解耦与可复现研究规划路线图 |
| [`ARCHITECTURE.md`](ARCHITECTURE.md) | 系统分层架构、实体时序图、崩溃对账与契约规约 |
| [`core/contracts.py`](core/contracts.py) | `ContentFact`、`NoteReference`、`FieldPresence`、`CrawlTaskManifest` |
| [`core/normalizer.py`](core/normalizer.py) | 数据清洗组件 `DataCleaner`（多词轨迹合并去重、正文完整性校验） |
| [`core/storage.py`](core/storage.py) | 持久化导出组件 `ObsidianExporter`（原子写入、保护用户批注）与 `CsvExporter` |
| [`core/checkpoint.py`](core/checkpoint.py) | 状态对账组件 `CheckpointStore`（崩溃指纹对账、跨进程引用持久化） |
| [`core/snapshot.py`](core/snapshot.py) | 完整任务快照 `xhs_task_snapshot_v1`（离线恢复、结构校验与来源链追踪） |
| [`core/control/`](core/control/) | 采集框架无关的持久运行控制（会话探测门禁、滚动时间窗配额、跨进程风险暂停与租约锁） |
| [`core/resilience.py`](core/resilience.py) | 请求配额预算、指数退避重试、断路器熔断与驱动故障转移路由 |
| [`core/pipeline.py`](core/pipeline.py) | 核心调度管线（纯净流程协调器，基于依赖注入） |
| [`tests/test_conformance.py`](tests/test_conformance.py) | 通用驱动一致性合规测试套件（验证引用失效与取消语义） |
| [`tests/test_pipeline.py`](tests/test_pipeline.py) | 9 项全流程单元与验收测试（多词去重、直查零虚构、崩溃对账等） |
