# OpenChamber 长时交互作业 — 后端改动说明（供后端开发人员评审）

> 评审入口文档：只列改动清单、评审关注点与验证状态；**机制细节、信号定义、参数流转、看门狗伪码见设计文档 `docs/openchamber-long-lived-session.md` §3/§4（以设计文档为准，此处不复述）**。
> 前端（会话网关）改动见 inkfront-code `docs/openchamber-long-lived-session-frontend.md`。
>
> **重要结论**：后端空闲回收（idle-kill）只覆盖 openchamber，**不覆盖 opencode**——opencode 的 run.sh 只有 deadline 看门狗、不读 `INK_IDLE_KILL_SEC`，且其 htc 配置无 `idle_kill_sec`。`+HepJob_Walltime` 修复对 opencode 中性（前后都是 `"default"`）。

## 1. 改动清单

| 仓库 | 提交 | 内容 |
|---|---|---|
| `fastink-code` | `bdbdfb7` + `9625b0c` + `88792ed` + 后续 | run.sh 四信号看门狗（信号 A/B/C/D）+ `XDG_DATA_HOME` 作业级隔离 + 信号 D 键盘/鼠标；utils.py per-app env 注入。**改动收敛于 openchamber 单 app：vnc/shell.sh/提交链不动**（评审 lib 去重建议经项目决定不采纳，CPU 探测函数为 openchamber 本地副本） |
| `fastink-plugins-ihep` | `68db0a0`（干净分支 `fix/hepjob-walltime-classad-clean`，仅含 walltime 修复） | `+HepJob_Walltime` 使用配置值，替换硬编码 `"default"` |
| `fastink-dev` | `3bb9f82` | `jobtype.openchamber.htc`：`init_hours: 168` / `idle_kill_sec: 86400` / `walltime: "long"` |

fastink-code 内逐文件：

- `apps/openchamber/run.sh` — 四信号看门狗（A WAL/db mtime、B tool-output、C CPU 增量、D 入站网络增量→心跳文件）；`OPENCHAMBER_DATA_DIR`/`XDG_DATA_HOME` 默认落 `$APP_PATH`（避 AFS 配额 + 按作业隔离）
- `computing/tools/common/utils.py` — per-app env 注入（`jobtype.<name>.htc.init_hours/idle_kill_sec`，未配置时行为不变）
- `tests/test_submit_env_injection.py` — 3 用例（默认/全覆写/仅 init_hours）
- `docs/openchamber-long-lived-session.md` — 设计文档（评审后已更新）

## 2. 评审关注点

1. **`XDG_DATA_HOME` 作业级隔离**（评审问题 1 修复）：opencode 只认 `XDG_DATA_HOME`；run.sh 默认 `${APP_PATH}/opencode-data`，站点 env 钩子可覆盖。**合并前需在 fastink-test 真实作业内确认 `opencode.db` 落点在作业目录**。副作用：会话历史不跨作业持久（与 openchamber-web 状态同一取舍）。
2. **信号 C 的 cgroup v2 / pidstat 依赖**：逐节点确认 cgroup v2 可用性；pidstat 兜底在生产从未执行过。两者皆不可用时 C 失效（退化 A/B/D，静默计算任务可能被误杀）。
3. **信号 D 的 `ss -ti bytes_received` 字段依赖**：老内核/iproute2（<3.10）无此字段时 D 静默失效（fail-safe，键盘保活失效但不误杀）。并入同一份逐节点检查清单。**短连接关闭后字节计数消失**——T9 必须用真实浏览器 + OpenResty/nginx 中继链路验证，合成流量测不出。
4. **阈值防呆**：`INK_ACTIVE_NET_MIN_BYTES`/`INK_ACTIVE_CPU_MIN_PCT` 均强制 `^[1-9][0-9]*$`（0 会致永不回收）。
5. **`OPENCHAMBER_DATA_DIR`/`XDG_DATA_HOME` 落作业目录后**：作业结束随 workfs 清理，数据不持久到 AFS 家目录——产品取舍需确认（用户应走文件同步/下载保留成果）。
6. **`walltime: long` 需池管理员确认**：与池侧 `PERIODIC_REMOVE` 分级策略匹配后才能生产启用。
7. **上生产顺序**：T9（真实链路键盘/鼠标验证）通过前，fastink-dev 配置 MR（`idle_kill_sec: 86400`）不上生产。

## 3. 验证状态

| 项 | 结果 |
|---|---|
| `test_submit_env_injection.py`（3 用例）+ 相邻回归（78 用例，dev 容器） | ✅ |
| openchamber/run.sh `bash -n` + 看门狗决策逻辑 4 case 提取测试（XDG 新路径） | ✅ |
| T7 闲置回收（加速值）/ T8 CPU 不误杀 | ✅ |
| 信号 D 端到端（活 socket + monitor 盖章，评审者实测） | ✅（合成环境） |
| **T9 真实浏览器链路键盘/鼠标** | ⏳ 待验证（上生产前必须） |
| 真实作业内 opencode.db 落点确认（`$APP_PATH/opencode-data`） | ⏳ 待验证（合并前） |

## 4. 分支状态

- `fastink-code`：`feature/openchamber-idle-lifecycle` → MR !155（本 MR）
- `fastink-plugins-ihep`：`fix/hepjob-walltime-classad-clean`（干净分支，仅 `68db0a0`；旧 `fix/hepjob-walltime-classad` 含未合并 BES feature `f5549dc`，已弃用，评审问题 2）
- `fastink-dev`：`feature/openchamber-longlived-config` → MR !32
