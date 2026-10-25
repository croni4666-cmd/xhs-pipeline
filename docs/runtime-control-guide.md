# 运行控制与替换采集框架

本功能降低重复登录和失控访问的工程风险；不保证账号不被限制。当前验证为离线测试、接口替身及本地子进程，实际 MediaCrawler/浏览器仍需在授权环境联调。

## 使用

默认启用 `require_runtime_control=true`。真实驱动缺少必要控制钩子时拒绝执行；显式布尔 `false` 仅用于不受这些保护的旧插件兼容，不能用字符串或数字替代。

同一浏览器/账号在更换框架后继续使用相同的 `control_platform` 与非敏感 `control_scope`，并共享同一 `checkpoint_dir`。范围由操作者配置，不是平台账号身份认证；不要将 Cookie、手机号或令牌作为范围名。

```bash
xhs-pipeline --settings config/settings.json --control-status --format json
xhs-pipeline --settings config/settings.json --acknowledge-pause --format json
```

两条命令只操作本地状态，不登录或访问平台。人工确认仅解除本地暂停，保留请求历史，下一次在线任务仍需探测；它不代表平台限制已经解除。离线 `--resume-state`、分析和导出不受在线暂停影响。

默认上限是每任务20次、每分钟6次、滚动24小时100次受控操作，均可在配置中调整。这些数字是工程预算，不是平台公布的安全阈值。`--max-budget` 仍计驱动尝试；完整浏览器资源请求总数未覆盖时仍记为未知。查看清单的 `budget_stats.runtime_control` 区分受控尝试数、持久状态与本次 `session_verified`。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| `core/control/contracts.py` | 通用状态、能力、策略和停止异常 |
| `core/control/store.py` | 范围摘要、持久暂停、请求时间窗与原子预留 |
| `core/control/runtime.py` | 任务租约、当前调用验证与策略执行 |
| 驱动适配器 | 平台/框架响应分类、会话探测、请求钩子和资源归属 |
| `core/pipeline.py` | 注入、编排、停止语义与既有清单记录 |

控制层不导入具体驱动、内容清洗、快照或卡片模块。平台业务码与日志正则仅留在适配器中；更换框架时无需修改内容事实和导出契约，也不改变已有快照格式。

## 新驱动的接入契约

1. 保留 `crawl_keywords(request)`，通过 `runtime_capabilities` 如实声明会话探测、请求钩子和风险事件能力。
2. 在 `prepare_run(runtime)` 接收上下文；管线已调用 `begin_attempt()`。过去的 READY 不能授权本次调用，必须使用既有会话完成本次有效性探测后 `mark_ready()`。
3. 在实际发出每个可观察请求的适配点调用一次 `await runtime.before_request()`；仅会话探测允许 `probe=True`。框架重试也必须重新通过该钩子；避免同时在高层和底层重复扣减同一次尝试。
4. 将登录失效、验证码、限流和账号限制映射为通用 `SessionState`，调用 `runtime.pause(state)` 停止。网络故障映射 NETWORK_ERROR 并终止当前操作，不启动登录流程。
5. 仅关闭本任务创建的页面/连接，不能关闭用户已有上下文或浏览器。保留已获取的部分事实，并通过现有部分响应路径记录来源与失败状态。
6. 如使用独立进程，通过 `bridge_config()` / `from_bridge_config()` 共享同一状态目录和范围；控制配置不包含凭证。当前 MediaCrawler 桥使用 stdin 配置及受限结构化事件，stdout/stderr 持续监测。

这些声明是驱动实现契约，不是对任意第三方插件的安全认证。平台专用采集库仍属于需要信任和单独验证的执行依赖。

## 已验证的关键边界

- 暂停跨实例/驱动保留，人工确认不清空配额，新任务不能使用旧 READY。
- 风险日志在零退出码时仍触发暂停，运行中停止并等待自有子进程退出。
- 登录准备阶段失败阻断备用驱动；限速等待后需重新验证状态。
- 当前有效页在达到配额时仍交付，后续搜索在本地终止。
- 中途停止保留完整笔记；被截断的 UTF-8/JSON 行按失败行记录，不替换字符伪造事实。
- 完整导出、快照和离线恢复继续使用原有模块与契约。
