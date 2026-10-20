# OpenChamber 长时交互作业：资源效率保活 + 独立会话网关

> 状态：设计文档（Implementation Plan）
> 适用版本：fastink-code `main`（2026-08），配合 inkfront-code 前端
> 作者：Jingyan Shi（shijy@ihep.ac.cn）
> 最后更新：2026-08-29（评审修订：修正事实错误；补充会话-用户绑定、`+HepJob_Walltime` 硬编码修复、`oc_ui_session` 过期处理；明确改动落点为 inkfront-code `opencode-direct.conf`）
>
> **文档标记约定**：文中方案分三种状态——
> ✅ 已实现（已提交代码）　🔧 待实现（本文给出设计）　⚠️ 待验证/待确认（开放问题，实施前需确认）

---

## 1. 背景与目标

### 1.1 问题

OpenChamber 是 INK 上的**长时交互式 AI 编程工具**（基于 opencode，经 8446 OpenResty 反向代理访问）。当前存在两个互相独立、但共同影响用户体验的问题：

1. **作业被固定时长杀掉**（资源侧）
   `job_time.walltime`（默认 24h）一到，`run.sh` 的看门狗无条件 `cli.js stop`。**用户正活跃使用时作业也照杀**——对 24 小时以上的长对话/长 agent 任务完全不可用。

2. **网页会话被全站登录态卡死**（会话侧）
   前端登录态是 RuoYi 风格的 `Admin-Token` cookie（由 inkfront **Python FastAPI** 后端签发，Redis `login_krb5tokens:<token>` 记录，TTL 24h，见 `inkfront-code/server/src/auth/auth.py:14-15`）。OpenResty 8446 从该 cookie 取用户身份并调 FastINK `resolve_job_proxy` 转发。**登录态过期 → 8446 拒绝转发 → 即使作业还活着，网页也连不上**。

### 1.2 目标（管理员视角）

- **资源效率**：作业的生死由"是否真的被使用"决定，而不是固定 24h。
  - 活跃（用户交互 / agent 在跑）→ 作业保持运行，最长不超过一个硬上限（如 7 天）。
  - 连续空闲超过阈值（如 24h）→ 主动回收作业，释放资源。
- **会话可用**：网页对 openchamber 的访问不依赖全站 JWT 的 24h 寿命。
  - openchamber 拥有独立的会话，**作业活着会话就在**。
  - 主站 JWT 过期 / 改密码 / 其他工具 24h 重登——都不影响已建立的 openchamber 会话。
- **安全可控**：不做"前端存密码自动重登"这类放大风险的方案；会话可独立吊销。

### 1.3 非目标

- 不改动其他交互工具（vscode / jupyter / vnc / opencode）的生命周期。
- 不做全站会话时长调整。
- 不引入额外的强认证/双因素。
- **不覆盖 opencode(8447)**：opencode 同为长时交互工具、同样受 JWT 24h 限制，但本方案仅覆盖 openchamber。opencode 未来可复用同一"票据 + 独立网关"模式（见 §10 演进项）。

---

## 2. 现状分析

### 2.1 OpenChamber 作业生命周期（现状）

```
提交时 (utils.py:436)                         运行时 (run.sh)
─────────────────────────                    ────────────────
job_time.walltime (默认24h)          →       DEADLINE = now + INK_INIT_HOURS*3600
env 注入 INK_INIT_HOURS                      死循环:
                                              ・now >= DEADLINE        → cli.js stop, exit 0
                                              ・端口消失               → exit 1
                                              ・每 900s 检查一次
```

**结论**：当前 `run.sh`（main 分支）无任何活动检测，到 24h 无条件自杀。另有一把"池侧"的刀：IHEP 计费 hook（`fastink-plugins-ihep/ihep_plugin/hooks/computing.py`）给作业打 `+HepJob_Walltime` ClassAd，condor 池按 default/mid/long 分级做 `PERIODIC_REMOVE`——即使脚本不退出，调度器也可能按时清掉。
**⚠️ 已核实的代码事实（影响 §4.3）**：`computing.py:62` 虽然读了 `jobtype.<name>.htc.walltime` 配置，但该值只拼进 `accounting_group`（L79）；**L84 的 `+HepJob_Walltime` 是硬编码 `"default"`**，配置值到不了这个 ClassAd。若池侧 `PERIODIC_REMOVE` 依据的是该 ClassAd，则只改配置无效，必须同时修复插件（见 §4.4）。

### 2.2 会话链路与时效（已核对 inkfront-code 仓库实现）

```
浏览器 ─①─ OpenResty :8446 ─②─ FastINK resolve_job_proxy ─③─ condor 作业 ─④─ opencode
```

| # | 层 | 机制 | 时效 | 配置位置 | 过期后果 |
|---|-----|------|------|---------|---------|
| ① | OpenResty 8446 鉴权 | `backend.auth()` 读 `Admin-Token` cookie → 查 Redis `login_krb5tokens:<token>` → 换 `{krb5token, afsaccount}` | 与登录态同步（24h） | `inkfront-code/deploy/openresty/nginx/lua/backend.lua` + `conf.d/opencode-direct.conf` | 8446 拒绝转发，需重登 |
| ② | FastINK token 校验 | 每次 `resolve_job_proxy` 验 Ink-Username/Ink-Token（`routers/v2/compute_resources.py:62`） | krb5 票据 | FastINK auth 后端 | 该次调用失败 |
| ③ | 前端登录态 | `Admin-Token` cookie + Redis `login_krb5tokens:<token>`（TTL 24h） | 24h | inkfront **Python FastAPI** 后端（`server/src/auth/auth.py:14-15`） | 全站 API 401，Redis token 被删 → 8446 失效 |
| ④ | 作业本身 | `job_time.walltime`=24h + 计费组 walltime | 24h | config.yml | **作业真死，不可恢复** |

**关键代码事实**（评审核实，替代原"实测发现"）：

- 8446 的完整现状实现在 **`inkfront-code/deploy/openresty/nginx/conf.d/opencode-direct.conf`**（在仓库内，**不是站点私有文件**）。`backend.lua` 只负责 cookie → Redis 身份解析（写 `ngx.var.username` / `ngx.var.token`）；resolve 子请求、缓存、pre-auth、转发目标构造全部在 `opencode-direct.conf` 的 `access_by_lua_block` 里。
- OpenResty **不解析 JWT payload**，而是拿 `Admin-Token` 作 key 查 **Redis**（`login_krb5tokens:<token>`）换取用户身份。该 Redis key 由 inkfront Python 后端在登录时写入，TTL 24h。
- OpenResty lua **已连接 Redis**（`backend.lua` 中 `red:connect()`，含 DNS resolve + AUTH + keepalive）——**Redis 是现成的 session 存储**，无需引入额外状态存储。但注意连接建立在 `auth()` 函数作用域内，session 逻辑需自建连接（可抽公共函数复用）。
- **现状已有两层本方案必须处理的机制**（原文档遗漏）：
  1. **shared-dict 600s 缓存**：`ngx.shared.opencode_auth`，key = `openchamber-direct:<username>`（按已验证用户隔离），带 `_ink_job_id` 匹配校验，502/504 时 `log_by_lua` 清除。与新 Redis session 功能高度重叠 → 本方案**替换**它（见 §5.7）。
  2. **`oc_ui_session` pre-auth**：首次访问时网关代 POST `/auth/session`（带 per-job password）换取 OpenChamber Express 自己的 UI JWT cookie，再 302 自跳转。**该 JWT 若有过期时间，7 天长会话必然撞上**（见 §5.8）。
- 登录态过期 → Python 后端清理 Redis 记录 → 8446 立即失效。

**结论**：层 ④ 是唯一不可恢复的——必须由本方案解决。层 ③/① 过期只导致"需要重新登录"，但会打断 8446 转发。方案要点：**④ 用空闲回收保活；① 用 Redis-backed 独立 session 解耦**（复用现有 Redis，不新发明 cookie session）。

### 2.3 opencode 活动信号可行性（已实测）

对 `~/.local/share/opencode/` 的 SQLite 库分析：

- `message` / `part` 表写入**高度稀疏**：最长 8222 分钟（≈5.7 天）无更新，之后又正常。**opencode 的后台系统消息不会持续写库**，写入 = 真实交互（用户消息 / agent 工具输出）。
- `part` 表在 agent 执行长任务时**密集写入**（一分钟 23 条 = 工具流式输出）。
- `opencode.db-wal` / `opencode.db` 的 mtime 随上述写入刷新。

**结论**：以文件 mtime 做"交互"信号可靠。但存在盲区——**纯 CPU 计算的静默长任务**（无输出、无写入）需用进程 CPU 增量兜底。

**⚠️ 待验证/待确认的开放问题**（影响方案正确性，实施前需在 worker 环境确认）：

1. **信号 B（`tool-output/`）未验证**：本机实测该目录为**空目录**。需在真实 openchamber worker 环境确认：长工具调用是否真的向 `tool-output/` 落盘、路径是否一致。若 opencode 版本不写此目录，信号 B 失效，需改用其他流式输出信号。
2. **信号 A/C 的实例隔离问题**：`~/.local/share/opencode/` 与 cgroup CPU 统计可能是**按用户共享**而非按作业隔离。**在 IHEP 该问题几乎必然成立**：`$HOME` 在共享文件系统上，同一用户跨节点的多个实例共享同一 `opencode.db`，信号 A 必串扰——一个实例活跃会让所有闲置实例永不回收。~~实施前需确认 opencode 数据目录能否隔离到 `$APP_PATH`。~~ **✅ 评审已定论（MR !155 review）**：opencode 只认 `XDG_DATA_HOME`，不认 `OPENCODE_DATA_HOME`（三仓库零处设置，外部核实 sst/opencode 上游）。**已按此修复**：run.sh 起 daemon 前 `export XDG_DATA_HOME="${XDG_DATA_HOME:-${APP_PATH}/opencode-data}"`（与 `OPENCHAMBER_DATA_DIR` 同构：按作业隔离、避开 AFS 配额、站点 env 钩子可覆盖），信号 A/B 扫描该路径；`OPENCODE_DATA_HOME` 仅作为站点钩子自定义路径的兼容扫描分支保留。合并前需在 fastink-test 真实作业内 `ls` 确认 opencode.db 实际落点在作业目录下；
   - cgroup v2 是否 per-job（HTCondor 通常 per-job）还是 per-user。

---

## 3. 整体方案

```
┌────────────────────────── 资源侧（作业保活）──────────────────────────┐
│  openchamber/run.sh 四信号看门狗                                     │
│    A. WAL/db 写入  OR  B. tool-output 流式输出  OR  C. 进程CPU增量  │
│    OR  D. 键盘/鼠标入站流量（后台监视器盖章心跳文件）                  │
│    ─ 活跃 → 继续跑（上限 init_hours，如 7 天）                       │
│    ─ 空闲 > idle_kill_sec（如 24h）→ 主动回收                        │
└──────────────────────────────────────────────────────────────────────┘
                                    │
┌────────────────────────── 会话侧（Redis 独立 session）───────────────┐
│  ① 首次：Admin-Token → Redis login_krb5tokens 验证（同现状）          │
│  ② 签发：Redis 存 openchamber_session:<sid> = {host,port,passwd,...} │
│           浏览器得到 openchamber_sid cookie（只是随机 key）           │
│  ③ 后续：读 cookie → Redis 查 session → 滑动续期 → 转发              │
│  ④ JWT 过期 / 改密码 → openchamber_sid 仍有效，直到作业回收           │
└──────────────────────────────────────────────────────────────────────┘
```

两条线都以"**作业存活**"为锚点，互不依赖全站登录态。会话存储复用现有 Redis（`backend.lua` 已连接），无需新增存储或引入 cookie 加密。

---

## 4. 后端设计（fastink-code）

> **后端改动范围**：§4.1-§4.3（run.sh + utils.py + 配置）+ §4.3.1（fastink-plugins-ihep 插件修复，评审新增）。会话侧全部由 OpenResty lua 在网关层实现（inkfront-code 仓库），**不新增 fastink-code 后端端点**。

### 4.1 openchamber/run.sh — 四信号空闲回收看门狗

> **实现状态**：✅ 已实现并实测（T7 闲置回收 / T8 CPU 不误杀均通过）。run.sh 现含 A/B/C/D 四信号 + 后台网络活动监视器。

**信号定义**：

| 信号 | 状态 | 探测方式 | 覆盖场景 |
|------|------|---------|---------|
| A | ✅ 已实现 | `opencode.db-wal` / `.db` mtime 新鲜 | 用户发消息、agent 每步写事件 |
| B | ✅ 已实现 | `tool-output/` 目录内文件 mtime 新鲜（无则无害） | 长工具调用流式输出 |
| C | ✅ 已实现 | cgroup v2 `cpu.stat usage_usec` 增量 ≥ 阈值（pidstat 兜底） | 静默 CPU 计算长任务 |
| D | ✅ 已实现 | app 端口入站字节增量（`ss -tin` 汇总 `bytes_received`），后台监视器盖章 `.ink_heartbeat` | **鼠标点击/键盘输入**（终端 UI 按键经 WebSocket 流式上送） |

**信号 D 设计说明**（键盘/鼠标交互检测）：

- 终端类 UI（xterm.js）会**逐键**把输入经 WebSocket 送到服务端，鼠标点击（开启 mouse report 时）同理——这些就是 app 端口的**入站数据**，`bytes_received` 只在真实输入时增长，空闲 keepalive ACK 不推进该计数。
- 主看门狗每 900s 才醒一次、且单次采样仅 5s，若在采样间隙发生短暂打字，逐次采样会漏掉。因此信号 D 用一个**常驻后台监视器**（`net_activity_monitor`，每 5s 采样一次入站字节增量，超过阈值 `INK_ACTIVE_NET_MIN_BYTES` 默认 64B 就 `touch $APP_PATH/.ink_heartbeat`），把活动**固化成文件 mtime**，供主看门狗下次醒来读取——与信号 A 的"mtime 持久记录"语义一致。
- 阈值作用：过滤浏览器/客户端周期性 keepalive ping（单帧仅数字节），避免"页面开着但无人操作"的作业永不回收；真正的持续输入在 5s 窗口内远超 64B。
- **已知盲区（诚实声明）**：纯鼠标**悬停/移动**（无点击、无键入、无页面请求）在终端 UI 里根本不上送服务端，服务端无法感知；聊天式 UI 若在文本域内"打了字但未回车"，同样不上送。要覆盖这类"客户端本地事件"，需**客户端心跳**——由 OpenResty 网关注入一段监听 `mousemove`/`keydown`（节流）的小 JS，定时发 beacon，再经 FastINK 落一次心跳记录到 worker 可读处（见 §10 演进项）。当前信号 D 已覆盖键盘与点击这两类最主流的交互。

**看门狗逻辑**（改造 `run.sh` 主循环，最终目标态）：

```bash
IDLE_KILL="${INK_IDLE_KILL_SEC:-0}"   # 0 = 关闭（向后兼容，行为同现状）
LAST_ACTIVE=$(date +%s)

is_active() {
    # A/B: 三个信号源的 mtime 取最新，推进 LAST_ACTIVE
    local newest=0 now=$(date +%s)
    for f in "$HOME/.local/share/opencode/opencode.db-wal" \
             "$HOME/.local/share/opencode/opencode.db" \
             "$HOME/.local/share/opencode/tool-output"/* \
             "${APP_PATH}/.ink_heartbeat"; do
        [[ -f "$f" ]] || continue
        m=$(stat -c %Y "$f" 2>/dev/null) || continue
        (( m > newest )) && newest=$m
    done
    (( newest > LAST_ACTIVE )) && LAST_ACTIVE=$newest
    # C: 进程 CPU 增量（cgroup 法）
    if _has_cpu_activity_cgroup; then LAST_ACTIVE=$(date +%s); return 0; fi
    (( now - LAST_ACTIVE < IDLE_KILL ))
}

while true; do
    now=$(date +%s)
    (( now >= DEADLINE ))        && stop && exit 0   # 硬上限
    ! port_alive                 && exit 1
    (( IDLE_KILL > 0 )) && ! is_active && stop && exit 0  # 空闲回收
    sleep "${INK_CHECK_INTERVAL:-900}"
done
```

**关键语义**：`is_active()` 四信号 OR，任一命中即活跃。纯计算任务由信号 C 保证不被误杀，纯键盘/点击交互由信号 D 保证不被误杀。

**实现注意**：
- 从 `vnc/run.sh` 复制 `_get_cgroup_v2_path` / `_has_cpu_activity_cgroup` / `_has_cpu_activity_pidstat`，统一阈值变量名 `INK_ACTIVE_CPU_MIN_PCT`（默认 5）。
- `APP_PATH/.ink_heartbeat` 预留显式心跳钩子（未来 opencode 扩展可写）。
- 向后兼容：`INK_IDLE_KILL_SEC` 未设置时行为与现状完全一致。
- **实例隔离（✅ 已按评审修复）**：run.sh 起 daemon 前 `export XDG_DATA_HOME="${XDG_DATA_HOME:-${APP_PATH}/opencode-data}"`——opencode 只认 `XDG_DATA_HOME`（上游核实，`OPENCODE_DATA_HOME` 为幽灵变量），默认落作业目录实现按作业隔离 + 避开 AFS 配额；信号 A/B 扫 `${XDG_DATA_HOME}/opencode`（兼容扫描站点钩子可能设置的 `OPENCODE_DATA_HOME`）。取舍：会话历史不跨作业持久（与 openchamber-web 状态的取舍一致）。

### 4.2 utils.py — 按 app 覆盖时长 + 注入 idle-kill 参数

> **实现状态**：🔧 设计待实现。

`computing/tools/common/utils.py` 中 **`generate_submit_command`**（L392 定义；**不是** `generate_condor_sync_submit`——后者在 L502，只做 htc 资源覆盖）内的 env 注入段（L432-438，`INK_INIT_HOURS` / `INK_CHECK_INTERVAL` / `INK_ACTIVE_IDLE_SEC` 三行，具体行号随 main 漂移）改造。per-app 覆盖模式直接复用同文件 `generate_condor_sync_submit` L555 已有范式（`get_config` 已确认支持 `fallback` 参数，见 `common/config.py:52`）：

```python
job_walltime = int(get_config("job_time", "walltime"))
watch_interval = int(get_config("job_time", "check_interval"))
active_idle = int(get_config("job_time", "active_idle"))

# per-app 覆盖：jobtype.<name>.htc.init_hours / idle_kill_sec
app_htc = get_config("jobtype", job_type, fallback={}).get("htc", {}) or {}
if app_htc.get("init_hours"):
    job_walltime = int(app_htc["init_hours"])

env_parts.append(env_kv("INK_INIT_HOURS", job_walltime))
env_parts.append(env_kv("INK_CHECK_INTERVAL", watch_interval))
env_parts.append(env_kv("INK_ACTIVE_IDLE_SEC", active_idle))
if app_htc.get("idle_kill_sec"):
    env_parts.append(env_kv("INK_IDLE_KILL_SEC", int(app_htc["idle_kill_sec"])))
```

### 4.3 配置项（config.yml / fastink-dev overlay）

```yaml
job_time:
  walltime: 24          # 其他 app 不变
  check_interval: 900
  active_idle: 1800

jobtype:
  openchamber:
    htc:
      init_hours: 168        # 硬上限 7 天
      idle_kill_sec: 86400   # 24h 无交互回收
      walltime: long         # 计费分级 → 池侧不做 24h PERIODIC_REMOVE
```

> **⚠️ 第 4.3 的 `walltime: long` 单独配置无效，必须配合插件修复**：已核实 `fastink-plugins-ihep/ihep_plugin/hooks/computing.py` 的实际行为——L62 读了 `jobtype.<name>.htc.walltime`，但只用于 `accounting_group`（L79）；**L84 的 `+HepJob_Walltime` ClassAd 是硬编码 `"default"`**（原文档"hooks/computing.py 将 walltime 写入 +HepJob_Walltime"的描述有误）。因此本节配置需配合 §4.3.1 的插件修复。此外**池侧是否真有 `long` 档、其实际允许时长是否 ≥ 7 天、以及池侧 `PERIODIC_REMOVE` 到底读 `+HepJob_Walltime` ClassAd 还是 `accounting_group` 尾段，均未确认**。实施前必须与池管理员核对，否则：池在 24h 先于脚本看门狗清除作业 → 脚本逻辑失效；long 档时长不够 7 天 → 硬上限实际被池侧截断。

### 4.3.1 fastink-plugins-ihep — 修复 `+HepJob_Walltime` 硬编码

> **实现状态**：🔧 待实现（新增，评审发现）。

`ihep_plugin/hooks/computing.py:84` 改为使用 L62 已读取的 `walltime` 变量：

```python
# 修改前（硬编码，配置值到不了 ClassAd）：
"+HepJob_Walltime": f"\"default\"",
# 修改后：
"+HepJob_Walltime": f"\"{walltime}\"",
```

注意该修复影响**所有** jobtype 的 ClassAd 生成——当前除 openchamber 外没有 jobtype 配置过 `htc.walltime`，缺省仍是 `"default"`，行为不变；但需在 MR 说明中标注该全局影响。

### 4.4 简化的实现路径

> 本方案不新增后端端点，不修改前端 Vue 代码，不引入票据。**会话存储复用现有 Redis**（`backend.lua` 已连接），无需新发明 cookie session。会话侧改动落在 **inkfront-code 仓库的 `deploy/openresty/nginx/conf.d/opencode-direct.conf`**（该文件在仓库内，走正常 MR 流程）。

**为什么可以这么简化？**
- 8446 端口与主站 INK 同属 `ink.ihep.ac.cn` 域名 → 浏览器自动把 `Admin-Token` cookie 发送给 8446
- OpenResty 的 `backend.auth()` 已经能读到 `Admin-Token` 并查 Redis 验证 → 转发
- **Redis 已经是 session 存储**（`backend.lua` 里 `red:connect()`），把 openchamber 的 session 放 Redis 是最自然的选择：
  - passwd 不离开服务端（cookie 只存随机 sid key，不存敏感信息）
  - 可单会话吊销（`DEL` Redis key 即可）
  - 支持滑动续期（`EXPIRE`）
  - 无需 HMAC 签名/加密库，实现简单

**与 cookie session 方案的权衡**（记录决策）：cookie session 方案（HMAC 签名 cookie 自身存 session 数据）虽省一次 Redis 访问，但 passwd 进 cookie（密钥泄露即所有 worker 密码泄露）、stateless 无法单会话吊销、实现需 HMAC/加密库。Redis-backed 方案利用现有基础设施，安全性与可控性更好，实现更简单。

---

## 5. 会话层设计（OpenResty Lua，inkfront-code 仓库）

> **改动落点修正（评审）**：原文档称"配置在站点私有 overlay、全部改动集中于 `backend.lua`"——**两者均不准确**。实际：
> - 8446 的完整逻辑在 **`inkfront-code/deploy/openresty/nginx/conf.d/opencode-direct.conf`**（在仓库内）；`backend.lua` 只做 cookie → 身份解析，且**不设置** `ngx.var.proxy_host/proxy_port/proxy_passwd` 之类变量（原 §5.4 伪代码引用的这些变量不存在）。
> - 本方案的 session 逻辑应改造 **`opencode-direct.conf` 的 `access_by_lua_block`**（可把 Redis session 读写抽成 `lua/` 下新模块如 `oc_session.lua` 供其 require）。`backend.lua` 保持不动。
> - 不涉及 fastink-code 后端与 Vue 前端代码。

### 5.1 架构

```
                ┌─────────────────────────────────────────────┐
                │             同一域名 ink.ihep.ac.cn          │
                │                                             │
浏览器 ──→ 主站 443 (RuoYi + FastINK)                          │
         ← Set-Cookie: Admin-Token (JWT, 24h)                 │
                │                                             │
浏览器 ──→ 8446 (OpenResty)                                    │
         ← Set-Cookie: openchamber_sid (随机 key, HttpOnly)    │
                │                                             │
Redis:                                                          │
  login_krb5tokens:<token>  → 现有 RuoYi 登录态 (~24h)          │
  openchamber_session:<sid> → 新方案独立 session (7d 滑动)      │
                │                                             │
                └─────→ 后端 nginx → worker:port               │
```

### 5.2 session 数据结构

| 项 | 值 |
|------|----|
| Redis key | `openchamber_session:<sid>`（`<sid>` = 生成的随机字符串，如 `oc_` + 32 字节随机 hex） |
| Redis value | JSON：`{"username": "shijy", "job_id": "12345", "host": "worker01", "port": "61234", "passwd": "<APP_PASSWD>"}` |
| Redis TTL | 7 天，**每次有效请求滑动续期**（`EXPIRE` 重置） |
| Cookie | `openchamber_sid=<sid>`，HttpOnly + Secure + SameSite=Lax + Max-Age=7d（滑动） |

**关键设计**：
- cookie 里**只有随机 sid**（不含 passwd/host 等敏感信息），所有 session 数据存 Redis。Redis key 本身不可猜测（随机 sid），即使泄露也无法直接获得 passwd。
- **sid 熵源**：OpenResty 无内置 uuid；用 `resty.random.bytes(32)` + hex 编码（或退而求其次 `ngx.var.request_id` 组合时间戳）。**不要**用 `math.random`。
- cookie 加 `Secure`（8446 是 SSL）。同域 cookie 会随请求发给 443/8447 等所有端口——无害（其他端口不读它），但需知晓。
- **session 必须记录 `username`**，用于 §5.3 的用户绑定校验（评审安全项 1）。

### 5.3 会话流程

```
首次请求（无 openchamber_sid）：
  1. 浏览器带 ?_ink_job_id=xxx 访问 8446
  2. 浏览器同时自动带 Admin-Token cookie（同域名）
  3. OpenResty backend.auth()：
     - 读 Admin-Token → 查 Redis login_krb5tokens:<token> → 得到 {krb5token, afsaccount}
     - 子请求 /internal/resolve-openchamber-job（现有 location）→ 得到 {host, port, passwd}
  4. 成功 → 生成随机 sid，Redis SET openchamber_session:<sid> = {username, job_id, host, port, passwd}, EX 7d
  5. Set-Cookie: openchamber_sid=<sid>; HttpOnly; Secure; SameSite=Lax; Max-Age=604800
  6. 走现有 oc_ui_session pre-auth（如无该 cookie）→ 转发到 worker

后续请求（有有效的 openchamber_sid）：
  0. 【用户绑定校验（评审安全项 1，必须）】若请求同时带有效 Admin-Token：
     backend.auth() 解析出的 username 与 session.username 比对，
     不匹配（共享浏览器换人登录）→ DEL session + 清 cookie → 走完整验证
     （无 Admin-Token 或已过期 → 跳过此检查，这正是本方案要支持的场景）
  1. 读 cookie openchamber_sid → Redis GET openchamber_session:<sid>
  2. 存在 → 取 {host, port, passwd} → Redis EXPIRE 7d（滑动续期）→ 转发
  3. 不再调 resolve_job_proxy，不依赖 Admin-Token
  4. 不存在（过期/被删）→ 兜底走完整 backend.auth()（现状流程）

登录态过期后：
  1. Admin-Token 对应 Redis 记录被 inkfront 后端清理 → 主站 401
  2. 但 openchamber_session:<sid> 仍在 Redis（TTL 独立）
  3. 8446 用 openchamber_sid 继续转发，不受影响（步骤 0 因无有效 Admin-Token 而跳过）
  4. 用户下次回主站时需重新登录，但 openchamber 不断

主站登出：
  1. 前端登出时应同步使 openchamber session 失效（评审安全项 1 的配套）：
     inkfront 后端登出接口顺带 DEL 该用户的 openchamber_session（需按 username 反查，
     可另存 openchamber_user:<username> → <sid> 索引 key，TTL 同步）
  2. 至少：前端登出时清除 openchamber_sid cookie（同域可清）
  3. §10 的 /openchamber/logout 端点由"演进项"提为本方案必做项之一

作业被回收后：
  1. 网关尝试转发 → worker 端口不可达 → 502
  2. log_by_lua（现有 502/504 处理位置）Redis DEL openchamber_session:<sid> → 会话失效
  3. 用户下次访问 8446 时需重新走完整流程

多作业同时开：
  1. 用户有作业 A 的 openchamber_sid（session 里 job_id=A）
  2. 用户再点作业 B 的 connect：URL 带 ?_ink_job_id=B（保留现有的数字格式校验）
  3. OpenResty 检查：URL 的 job_id 与 session.job_id 不同 → 视为"新作业" → 重新走完整 backend.auth() 验证
  4. 签发新的 openchamber_sid（覆盖旧的，cookie 同名更新）→ 转发到 B
  5. 用户回到 A：URL 带 ?_ink_job_id=A → session.job_id=B 不匹配 → 重新验证 → 再签发 A
     （即同一时刻只能有一个 active session，多开时互相覆盖；与现状 shared-dict 缓存行为一致；
      如需多开，cookie 名带 job_id 后缀）
```

### 5.4 关键逻辑（opencode-direct.conf 改造）

> **落点修正（评审）**：原伪代码写在 backend.lua、引用了不存在的 `ngx.var.proxy_host/proxy_port/proxy_passwd`。实际改造 `opencode-direct.conf` 的 `access_by_lua_block`：现状"第 2 步 cache-or-fetch"（shared-dict）替换为 Redis session（见 §5.7），其余步骤（auth 重定向、pre-auth、target_upstream 构造）保留。Redis 读写抽成新模块 `lua/oc_session.lua`（自建连接，复用 backend.lua 的 DNS resolve + AUTH + keepalive 模式）。

```lua
-- opencode-direct.conf access_by_lua_block 内（示意；oc_session 为新模块）
local oc_session = require "oc_session"   -- get/set/del/expire 封装

local sid = ngx.var.cookie_openchamber_sid
local url_job_id = ngx.var.arg__ink_job_id
if url_job_id and not url_job_id:match("^%d+$") then      -- 保留现有校验
    return ngx.exit(ngx.HTTP_BAD_REQUEST)
end

local session = sid and oc_session.get(sid) or nil

-- 0a. 多作业检查：URL 带不同 job_id → 作废当前 session
if session and url_job_id and url_job_id ~= tostring(session.job_id or "") then
    oc_session.del(sid); session = nil
end

-- 0b. 用户绑定检查（评审安全项 1）：带有效 Admin-Token 时校验归属
if session then
    local backend = require "backend"
    local ok_user = backend.try_resolve_user()   -- 新增：软解析，失败返回 nil 不 exit
    if ok_user and ok_user ~= session.username then
        oc_session.del(sid); session = nil       -- 换人了，作废
    end
end

local host, port, passwd
if session then
    -- 1. 有效 session：滑动续期 + 直接使用
    oc_session.touch(sid, 7 * 86400)
    host, port, passwd = session.host, session.port, session.passwd
else
    -- 2. 兜底：完整验证（现状流程：backend.auth() + resolve 子请求）
    local backend = require "backend"
    backend.auth()
    if ngx.var.username == "" or ngx.var.token == "" then
        -- （保留现状：导航请求 302 /login，其余 401）
        return ngx.exit(ngx.HTTP_UNAUTHORIZED)
    end
    local res = ngx.location.capture("/internal/resolve-openchamber-job", {
        method = ngx.HTTP_GET,
        vars = { username = ngx.var.username, token = ngx.var.token }
    })
    -- （保留现状：非 200 → "No Running Job" 页面）
    local data = require("cjson").decode(res.body)
    host, port, passwd = data.host, data.port, data.passwd
    -- 3. 签发新 session
    local new_sid = oc_session.new_sid()          -- resty.random 32B hex
    oc_session.set(new_sid, {
        username = ngx.var.username, job_id = url_job_id,
        host = host, port = port, passwd = passwd,
    }, 7 * 86400)
    ngx.header["Set-Cookie"] = "openchamber_sid=" .. new_sid
        .. "; HttpOnly; Secure; SameSite=Lax; Max-Age=604800; Path=/"
end

-- 4. oc_ui_session pre-auth（保留现状逻辑，passwd 来源改为上面的变量；§5.8）
-- 5. target_upstream 构造（保留现状）
```

`log_by_lua_block` 的 502/504 处理相应从 `shared:delete("openchamber-direct:...")` 改为 `oc_session.del(sid)`。

### 5.5 会话回收边界

| 事件 | 行为 | 时效 |
|------|------|------|
| 登录态 24h 过期 | openchamber_session 不受影响，继续转发 | 即时 |
| 用户改密码 | 下次回主站需重登；openchamber_session 不受影响 | 待作业断开（见下行） |
| 作业被回收 (idle-kill) | 端口不可达 → 502 → log_by_lua DEL openchamber_session | 最慢一个 check_interval (900s) |
| **共享浏览器换人**（评审新增） | 新用户带有效 Admin-Token → 用户绑定校验不匹配 → DEL session 重验 | 即时 |
| **主站登出**（评审新增） | 登出接口 DEL session（经 username 索引）+ 前端清 cookie | 即时 |
| 用户主动登出 openchamber | `/openchamber/logout`（本方案必做，见 §5.3） | 即时 |
| 7 天无任何交互 | Redis TTL 到期自动删除，需重新走完整验证 | 7d |
| 管理员封禁账号 | 作业被回收 → 端口不可达 → session 失效；或运维直接 DEL Redis key | 待作业断开 / 即时 |
| 多作业切换 | URL job_id 不匹配 → 旧 session 作废，重新验证签发新 session | 即时 |

### 5.6 安全考虑

| 关注点 | 措施 |
|--------|------|
| session 伪造 | Redis 随机 sid（`resty.random` 32 字节，不可猜测）；无签名/加密依赖 |
| session 盗用（XSS） | HttpOnly，JS 不可读；SameSite=Lax 防 CSRF；Secure 仅走 TLS |
| **跨用户串号（共享浏览器）** | session 记录 username；带有效 Admin-Token 时强制比对，不匹配即作废（§5.3 步骤 0）；主站登出联动清理 |
| passwd 泄露 | passwd 仅在 Redis 服务端，cookie 只存随机 sid；网关转发时注入 |
| session 重放 | Redis 可精确吊销（DEL）；滑动 TTL 限寿命 |
| 密钥泄露 | 无 HMAC 密钥，不存在密钥泄露风险；Redis 本身有 AUTH 保护 |
| 多作业混淆 | URL job_id 与 session.job_id 比对，不匹配则作废重验 |

### 5.7 与现有 shared-dict 缓存的关系（评审新增）

现状 `opencode-direct.conf` 已有 `ngx.shared.opencode_auth` 的 600s 缓存（key = `openchamber-direct:<username>`，带 job_id 校验、502/504 清除）。它与 Redis session 功能重叠，**本方案将其替换**（两层缓存并存会导致失效语义打架：一层删了另一层还在）。

- 替换后每个请求多一次 Redis GET+EXPIRE。openchamber UI 静态资源请求密集，若压测显示 Redis 往返成为瓶颈，可在 Redis 前加一层**短 TTL（如 30s）shared-dict** 作只读加速——但失效以 Redis 为准，shared-dict 仅作降压，且 502/504 时两层同时清。
- 首版实现建议**不加**这层加速，先量后优。

### 5.8 oc_ui_session 过期处理（评审新增，现状已有隐患、7 天会话放大）

现状 pre-auth 只在浏览器**没有** `oc_ui_session` cookie 时执行。若 OpenChamber Express 签发的该 JWT 有过期时间，过期后网关看见 cookie 存在便直接转发 → Express 层 401，用户卡死。24h 会话周期内可能碰不到；**7 天会话几乎必然碰到**。

处理：
1. 实施前确认 `oc_ui_session` JWT 的实际寿命（OpenChamber Express `/auth/session` 实现）。
2. 网关处理上游 401/403：清除 `oc_ui_session` cookie 并 302 自跳转，触发重新 pre-auth（session 里有 passwd，重新 pre-auth 无需用户参与）。可在 `header_filter_by_lua` / error_page 拦截实现。
3. 验证项加入 §8.2。

---

## 6. 安全考虑（汇总）

| 关注点 | 措施 |
|--------|------|
| 密码不在前端留存 | 独立 Redis session 方案，密码不存前端、不自动重登 |
| 会话盗用 | HttpOnly + Secure cookie；passwd 存 Redis 不进 cookie；可精确吊销 |
| **共享浏览器跨用户串号** | session 绑定 username，带有效 Admin-Token 时强制比对；主站登出联动清 session（§5.3/§5.5） |
| 管理员撤销 | 封禁 → 作业回收 → 端口不可达 → session 失效；或主动 DEL Redis key |
| 作业资源失控 | 硬上限 `init_hours`(7d) + 空闲回收 `idle_kill_sec`(24h) 双保险 |
| 信号误杀风险 | 四信号 OR，CPU 增量覆盖静默长任务、网络增量覆盖键盘/鼠标；`INK_IDLE_KILL_SEC=0` 可完全关闭该特性 |

---

## 7. 实施步骤（建议顺序）

| 步骤 | 内容 | 涉及 | 状态 | 验证 |
|------|------|------|------|------|
| 0 | 确认开放问题（§2.3）+ 池侧 walltime 机制（§4.3） | — | ⚠️ 前置 | worker 环境确认：tool-output 是否落盘；数据目录隔离变量名；cgroup 是否 per-job；池管理员确认 long 档与 PERIODIC_REMOVE 依据字段 |
| 1 | run.sh 信号 B/C（含 vnc CPU 检测移植）+ 数据目录隔离 | fastink-code | 🔧 待实现 | `bash -n`；在 fastink-test 提交 openchamber 作业观察日志 |
| 2 | utils.py `generate_submit_command` per-app init_hours/idle_kill_sec | fastink-code | 🔧 待实现 | pytest + 提交作业看 env |
| 3 | 修复 `+HepJob_Walltime` 硬编码（§4.3.1） | fastink-plugins-ihep | 🔧 待实现（评审新增） | pytest + `condor_q -l` 看 ClassAd |
| 4 | 配置（jobtype.openchamber.*，含 walltime:long） | fastink-dev overlay | 🔧 待实现 | 提交作业验证 `INK_IDLE_KILL_SEC` 与 ClassAd |
| 5 | opencode-direct.conf 换 Redis session（含用户绑定、oc_session.lua、502 清理、oc_ui_session 401 处理） | inkfront-code（deploy/openresty，仓库内） | 🔧 待实现 | 打开 openchamber → 检查 cookie → 清 Admin-Token → 页面仍可用 |
| 6 | 主站登出联动清理 openchamber session | inkfront-code（server + ui） | 🔧 待实现（评审新增） | 登出后 8446 需重新验证 |
| 7 | 端到端验证（含多作业切换、作业回收、换人串号） | 全链路 | 🔧 待实现 | 见 §8.2 |

---

## 8. 验证方案

### 8.1 作业保活（fastink-test 栈）

1. 配置 `jobtype.openchamber.htc.init_hours=168, idle_kill_sec=300`（临时用小值加速验证；生产用 86400）。**注意**：空闲回收的判定粒度是 `INK_CHECK_INTERVAL`（默认 900s）——加速验证时需同时调小 `job_time.check_interval`（如 60），否则最坏要等一个完整 check 周期才触发回收。
2. 提交 openchamber 作业 → 确认 env 含 `INK_IDLE_KILL_SEC=300`、`INK_INIT_HOURS=168`；`condor_q -l` 确认 `HepJob_Walltime` ClassAd 为配置值（验证 §4.3.1 修复）。
3. 模拟交互（向 opencode 库写事件 / 触碰 db-wal）→ 作业保持，不被回收。
4. 停止交互 300s → 作业被 `cli.js stop` 回收，日志出现 `No interaction ... stopping (idle-kill)`。
5. 模拟静默 CPU 任务（作业内跑 `while :; do :; done`）→ 信号 C 保持活跃（实现信号 C 后验证）。
6. 验证信号 A/C 的实例隔离（对应 §2.3 开放问题 2）：**跨节点**同时开两个 openchamber 作业（$HOME 共享文件系统场景），一个闲置一个活跃，观察闲置者是否被误判活跃。

> 说明：openchamber 是 fastink-code 的 computing app，**无 `dry_run` 概念**（`dry_run` 是 BES 插件的配置）。空闲回收靠 `idle_kill_sec` 调小来加速验证。

### 8.2 会话独立

1. 打开 openchamber → 确认浏览器收到 `openchamber_sid` cookie（HttpOnly + Secure）；确认 Redis 中有 `openchamber_session:<sid>`。
2. 模拟/等待登录态过期（手动清除 `Admin-Token` cookie 或等 24h）→ 页面仍可正常操作，openchamber 转发不断；确认 Redis `login_krb5tokens:<token>` 已删除，但 `openchamber_session` 仍在。
3. 关闭/回收作业 → 下次访问 8446 时，`openchamber_session:<sid>` 被删除，需重新走验证流程。
4. **多作业测试**：开作业 A → 再开作业 B → 确认连接的是 B 而非 A（session 被正确覆盖）。
5. **吊销测试**：手动 `redis-cli DEL openchamber_session:<sid>` → 下次请求走完整验证流程。
6. **换人串号测试（评审新增，必须）**：用户 A 建立 openchamber session → 主站登出 → 用户 B 登录 → B 访问 8446 → 必须**不能**进入 A 的作业（用户绑定校验 + 登出联动，二者至少一个生效）。
7. **oc_ui_session 过期测试（评审新增）**：手动删除/篡改浏览器 `oc_ui_session` cookie（模拟过期）→ 访问 8446 → 网关应自动重新 pre-auth，用户无感恢复，而不是卡在 Express 401。

---

## 9. 附：放弃的方案（决策记录）

| 方案 | 结论 | 原因 |
|------|------|------|
| 前端每 23h 用记住的密码调 clusterlogin 续 token（方案 A） | 放弃 | 密码留前端 + 私钥在源码 = 放大泄露风险；不符合独立吊销目标 |
| 双 token（access+refresh）改造登录体系 | 放弃（为此场景过重） | 需改 inkfront Python 后端 + Vue 前端全站登录流程，只为一个工具不值得 |
| 一次性票据 + 独立子域名网关 | 放弃（简化为此方案） | 同一域名可省票据、省新端点、省前端改 |
| HMAC 签名的 cookie 自身存 session（cookie session） | 放弃 | passwd 进 cookie（密钥泄露风险）、stateless 无法单会话吊销、实现需 HMAC 库；Redis 已是现成存储，更优 |
| 仅延长 job_time.walltime 到 48h | 部分采纳 | 固定时长仍不区分"是否在使用"，浪费资源；改为空闲回收更优 |

---

## 10. 演进项（未来）

1. **opencode(8447) 复用本方案**：opencode 同为长时交互工具、同样受 JWT 24h 卡死。OpenResty 可为 8447 端口用同一套 `openchamber_session` 逻辑（或独立 Redis key 前缀 `opencode_session:`），只需在 lua 中判断端口。
2. **显式心跳钩子**：若 opencode/openchamber 后续支持显式心跳文件，信号 A 可改为读 `$APP_PATH/.ink_heartbeat`，精度高于 WAL mtime。
3. ~~登出接口~~ **已提为本方案必做项**（评审安全项 1 配套，见 §5.3"主站登出"与 §7 步骤 6）。
4. **网关侧主动作业探测**：网关定时直连 FastINK 查询作业状态，作业回收后立即删 session（而非等 502）。
5. **Redis 前置 shared-dict 加速**：若量测显示 Redis 往返成为瓶颈，按 §5.7 加短 TTL 只读缓存。
6. **客户端鼠标/键盘心跳（覆盖信号 D 盲区）**：OpenResty 网关（`opencode-direct.conf`）用 `sub_filter` 注入一段监听 `mousemove`/`keydown`（节流）的小 JS，在用户活动时发轻量 beacon；beacon 经代理打到 worker，或由 FastINK 端点落一条时间戳记录到 worker 可读位置，由信号 D 的 `net_activity_monitor` 或 `.ink_heartbeat` 捕获。可把"纯鼠标悬停/聊天框未回车打字"也纳入活动判定。
