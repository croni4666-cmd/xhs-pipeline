# xhs-pipeline

小红书采集调度、数据清洗与 Obsidian 导出管线。当前版本为 **0.3.2rc3**，仍属预发布；本分支包含 RC2/RC3 审查后的修复，未发布 Stable。

## 驱动能力

| 驱动 | 当前行为 | 使用条件 |
| --- | --- | --- |
| mediacrawler | 调用独立安装的 MediaCrawler，导入本次新增或修改的 JSONL | 已配置本地路径、解释器与 CDP 浏览器；不支持直接详情查询 |
| http | 确定性离线模拟数据；没有真实平台 HTTP 采集实现 | 显式允许模拟数据 |
| mock | 离线测试样本 | 显式允许模拟数据 |

生产默认无备用驱动，且 `allow_synthetic=false`。模拟结果、任务清单与卡片携带 `is_synthetic` 和 `data_source`。健康检查只检查本地驱动条件，不证明平台采集成功。

## 安装与使用

需要 Python 3.10 或以上。

```bash
python -m pip install .
xhs-pipeline --help
```

将 `config/settings.example.json` 复制为本地 `config/settings.json`，设置 MediaCrawler 路径、解释器与 CDP 端口。安装包也包含默认配置。

真实驱动示例：

```bash
xhs-pipeline --settings config/settings.json --driver mediacrawler --keywords "杭州美食,杭州旅游攻略" --limit 20 --sort latest --max-budget 5 --export-obsidian ./obsidian_cards --export-csv ./data/summary.csv --format json
```

离线演示（不会采集平台）：

```bash
xhs-pipeline --driver mock --allow-synthetic --keywords "演示" --limit 2 --state-file ./data/demo.json --export-obsidian ./obsidian_cards --format json
```

从已有原始 JSONL 导出：

```bash
xhs-pipeline --resume-from ./data/search_contents.jsonl --export-obsidian ./obsidian_cards --format json
```

`--state-file` 保存原子写入的扁平 JSON 快照，保留字段状态和数据来源；它不是包含全部洞察与发现轨迹的任务恢复格式。`--resume-from` 接受原始 JSONL。含模拟标记的缓存也必须显式允许模拟数据。

## 数据与恢复语义

- `NoteReference` 承载临时访问上下文；持久化仅保留公开引用元数据，所有不透明凭证均移除。依赖凭证的引用重启后须重新搜索。
- `ContentFact` / `UnifiedNote` 与凭证分离，`DiscoveryRecord` 保存各次发现的关键词、排名和时间。
- 正文、媒体和指标保留五种 `FieldPresence`。未知正文完整性为 `unknown`，未采集不等同于空内容或零互动。
- 任务清单记录有效配置指纹、驱动版本、尝试、去重、停止原因和内容哈希。外部子进程内部请求数未受监测时为 `null`。
- `--max-budget` 与 `--rate-limit` 的单位是**驱动调用尝试**，包含重试；不是 MediaCrawler 内部 HTTP 请求数。
- 卡片使用固定身份文件名、生成区与人工批注区。相同生成输入不重写；更新前保留旧文件备份。身份冲突、读取失败或边界歧义会阻断覆盖。
- 对账分别验证事实哈希、完整渲染输入和磁盘实际字节哈希；不能仅凭卡片头中的哈希认定导出成功。

这些记录支持追溯采集过程，不能保证动态平台返回相同样本，也不能消除研究选择偏差。作者 ID 与昵称不自动匿名化；研究发布前应按具体数据授权和隐私要求处理。

## 验证

```bash
python -m pip install ".[dev]"
python -m pytest -q
python -m pip wheel . --no-deps --wheel-dir dist
```

回归包含配置多轮注入、凭证输出、CSV 公式、字段状态、批注保留、崩溃对账、并发提交、失败/取消清单、熔断回退和子进程生命周期。CI 配置还验证 Windows/Linux、Python 3.10/3.12 与安装后的 CLI。实际平台采集不在离线测试范围内。

详见 [安全边界](SECURITY.md)、[修复说明](docs/audit-repairs.md) 与 [架构](ARCHITECTURE.md)。许可证为 [MIT](LICENSE)。
