# OpenChamber/OpenCode 数据分层与持久化改造方案

> 状态：已实现，评审/合并中（fastink-code !182 + fastink-dev !53）。面向后端/部署开发人员。
> 相关文档：`docs/openchamber-long-lived-session.md`、`docs/openchamber-long-lived-session-backend.md`。
> 注意：`openchamber-long-lived-session-backend.md` 中"XDG_DATA_HOME 作业级隔离"一节已被 `d508951` 回退（opencode db 回到 `~/.local/share/opencode`），本文以回退后的现状为准。

## 0. TL;DR

问题：**想持久化的 openchamber 状态（project 注册表 / 定时任务 / AI memory / chats）落在每作业的临时目录，随作业孤儿化**——每个新作业的会话列表/项目从零开始，看起来像"历史丢了"。session 内容本身在共享 opencode.db 里，没丢。

本方案：

1. openchamber-web 状态从"每作业"改为"每用户固定目录"（workfs2 上的 `.ink/openchamber-data`，KB~MB 级），首次启动把最近一次作业的旧状态**复制**过去（copy-only，源不动、可跳过自配用户）；
2. 大体积/可再生数据（chats、tool-output）拆到 scratchfs2 的每用户 `.ink` 下，避免占用 workfs2 的小配额；
3. **opencode.db 留在 scratchfs2**：实测已有用户达到 ~4GB，而 workfs2 每用户配额仅 ~5GB——放不下，因此不做迁移；增长改由"作业启动维护"控制（条件 vacuum + 90 天保留 + 归档兜底）；
4. flock 实例锁把"每用户单 openchamber 作业"从后端约定变成文件系统强制；
5. 文件系统操作全部带超时，病态 FS 快速失败而不是挂住启动。

## 1. 背景与现状

### 1.1 存储介质（IHEP）

| 介质 | 空间 | 备份 | 备注 |
|---|---|---|---|
| scratchfs2 | 大 | **无** | 临时目录定位 |
| workfs2 | 有限（每用户 ~5GB） | **有** | 快盘 |
| AFS home | 小 | 有 | **SQLite 文件锁不兼容**（preflight 已拒跑） |

### 1.2 数据画像

| 数据 | 大小 | 性质 |
|---|---|---|
| `~/.local/share/opencode/opencode.db` | 样本 328MB（5307 message / 20568 part / 75 session）；**实测已有用户达 ~4GB**，只增不减 | 核心资产 |
| opencode `log/`、`tool-output/` | 18MB+ | 可再生 |
| openchamber-web 全部状态 | 632KB（其中 logs 440KB） | settings/projects/凭证 共 KB 级 |

### 1.3 当前落点

| 数据 | 当前位置 | 评估 |
|---|---|---|
| opencode.db（session 内容） | 自动迁移用户 → **scratchfs2**（`code_datadir_root=/scratchfs2/{group}/{user}/.ink/opencode`）；手工迁移用户（如 hanx）→ workfs2；未迁移 cc 用户 → ihepfs；AFS 用户 → preflight 拒跑 | **保持现状**：workfs2 每用户 ~5GB 装不下 ~4GB 且持续增长的 db；无备份是可接受的取舍，增长由 D6 维护闭环控制 |
| openchamber settings.json（project 注册表）、projects/（定时任务/memory）、sessions-directories、sessions-archive、jwt-secret | 每作业 `${APP_PATH}/openchamber-data`（workfs2 上的 `.ink/Jobs/htcondor/openchamber-<ts>/`） | **作业结束即孤儿**；新作业注册表从零开始（分组/归档/定时任务全部丢失） |
| openchamber logs/、run/、chats/ | 同上，每作业 | 合理（临时性），但固定化后 logs/chats 会累积，需保留策略 |

### 1.4 单实例前提（已验证成立）

- `jobtype.openchamber` 只有 `htc:` 段（config.ihep.yml），不跨集群双开；
- 前端 `hasExistingJobs(job_type, cluster_id)` 已有作业直接跳转；
- 后端 `_need_dedup`（ASYNC）：Redis 提交队列 + `find_active_jobs` DB 双查，重复提交返回 DUPLICATE。

前提成立 → 共享一份每用户目录没有常态化多写者；新旧作业交替窗口（DB 状态刷新延迟）仍可能短暂双实例，故补 D4 实例锁。

## 2. 目标 / 非目标

**目标**
- 明天的作业直接看到今天的 project 注册表、session 分组、归档状态、定时任务、AI memory；
- db 维持 scratchfs2（配额实测决定），以维护闭环控制其增长、避免写满导致会话写入失败；
- workfs2 空间可持续（chats/tool-output 下沉 + 清理闭环）；
- 上游（fastink-code，GPLv3）只加通用机制，IHEP 路径全部下沉 overlay；**上游零配置部署的默认行为与现状一致**（见 §4 末审计表）。

**非目标**
- 不做多实例并发支持（仍限定每用户一个 openchamber 作业）；
- 不做 session 归档的自动恢复 UI（归档仅人工可读的兜底导出，见 D7）；
- 不动 opencode CLI（登录节点直跑）写同一 db 的既有行为。

## 3. 目标布局

```
workfs2（有备份，每用户配额 ~5GB）— 注意两条不同的根：
  /workfs2/{user_group}/{user}/.ink/openchamber-data/   ← OPENCHAMBER_DATA_DIR（由 computing.ink_dir 推出，{user_group}，KB~MB 级）
  IHEP 上 {user_group} 与 {experiment_group_lower} 可能不同（如 u07 vs cc），两者不可混用。

scratchfs2（大、无备份）：
  /scratchfs2/{experiment_group_lower}/{user}/.ink/opencode/        ← code_datadir_root；opencode.db + auth.json + snapshot/ + storage/
      （~/.local/share/opencode symlink 指向这里；**维持现状，不迁移**——~4GB 的 db 装不进 5GB 的 workfs2）
  /scratchfs2/{experiment_group_lower}/{user}/.ink/openchamber-chats/     ← OPENCHAMBER_CHATS_DIR（上游原生支持）
  /scratchfs2/{experiment_group_lower}/{user}/.ink/opencode-tool-output/  ← run.sh 建 symlink：~/.local/share/opencode/tool-output → 这里
  /scratchfs2/{experiment_group_lower}/{user}/.ink/session-archive/       ← 旧 session 归档导出（D7）

每作业（临时，保持现状）：
  ${APP_PATH}/openchamber-data/run/、logs/、旧 chats/ 的语义不随 DATA_DIR 固定化而改变——
   Q3（已决）：run/ 与 logs/ 均留在 DATA_DIR——run/ 靠 openchamber 自清 + D4 实例锁，logs/ 靠 D6 保留策略。
```

## 4. 详细设计

### D1 每用户固定 OPENCHAMBER_DATA_DIR（fastink-code，run.sh）

```bash
# 现逻辑：export OPENCHAMBER_DATA_DIR="${OPENCHAMBER_DATA_DIR:-${APP_PATH}/openchamber-data}"
# 改为（per-user 模式自动推导；用户 env.sh 自配的值优先）：
OPENCHAMBER_DATA_MODE="${OPENCHAMBER_DATA_MODE:-per-job}"     # 上游默认保持 per-job，行为不变
if [ "${OPENCHAMBER_DATA_MODE}" = "per-user" ] && [ -z "${OPENCHAMBER_DATA_DIR:-}" ]; then
    OPENCHAMBER_DATA_DIR="$(realpath -m "${APP_PATH}/../../../openchamber-data")"  # <ink_dir>/.ink/openchamber-data
    INK_DATA_DIR_DERIVED=1
fi
export OPENCHAMBER_DATA_DIR="${OPENCHAMBER_DATA_DIR:-${APP_PATH}/openchamber-data}"
```

- 上游默认 `per-job`，对无此需求的站点零影响；IHEP overlay 设 `jobtype.openchamber.env.OPENCHAMBER_DATA_MODE: "per-user"`。
- `jobtype.<name>.env` 值**不支持占位符**（按 utils.py 的 per-app env 注入逻辑原样导出），故路径只能在 run.sh 运行时推导。
- **已自行配置的用户优先、零影响**：`[ -z ]` 守卫让 env.sh 里已 export 的用户继续用其自选位置；D1 只兜"没配过"的用户。
- 该方向已被手工 workaround 现地验证（用户在 `.ink/envs/opencode/env.sh` 里 export `OPENCHAMBER_DATA_DIR`，跨作业保留会话）。

### D1.1 首次启动播种（Q5 已决：采纳迁移，copy-only 硬保证）

per-user 模式首次启动时，把最近一次作业目录里的 openchamber-data **复制**进固定目录，保证注册表/定时任务/AI memory/历史 chats 连续。三条硬保证：

1. **绝不删除用户内容**：全程 `cp -a` 复制，源作业目录原封不动；播种失败只警告、不阻塞启动（固定目录从空白开始，历史仍留在旧作业目录）；
2. **用户自配固定目录的跳过**：`INK_DATA_DIR_DERIVED` 未置位即不播种；
3. **一次性**：`.seeded` 戳记 + 目标已有 `settings.json` 即跳过，绝不覆盖已有状态。

实现要点（run.sh，flock 之后、维护脚本之前）：扫描**两种**作业目录布局（`Jobs/*/openchamber-data` 嵌套 与 `Jobs/*/*/openchamber-data` 旧扁平）取 mtime 最新源 → `du` 估算拷贝预算 → 先拷到 `<固定目录>.seed-tmp-$$` 暂存 → 剔除暂存副本的 `run/` → 拷入固定目录 → 清理暂存。**候选扫描超时（rc=124）时不写 `.seeded`**，下次作业重试（区分"确认无候选"与"探测失败"）。

### D2 （已撤销）opencode db 迁至 workfs2

**决策：不迁移，db 留在 scratchfs2。** 实测已有用户的 `opencode.db` 达到 **~4GB**，而 workfs2 每用户配额仅 **~5GB**——装不下，且 db 只增不减，放上去很快会写满并导致会话写入失败。

配套（取代"挪到有备份层"的思路）：

- 增长控制交给 **D6 作业启动维护**：条件 `wal_checkpoint + VACUUM` + 90 天会话保留（先归档后删除）；
- `code_datadir_root` 维持 `/scratchfs2/{experiment_group_lower}/{username}`（overlay 不变）；
- 因此不引入"legacy root 再迁移"机制：搬迁助手、`code_datadir_legacy_roots` 配置、`INK_CODE_DATA_ROOT/INK_CODE_DATA_LEGACY_ROOTS` 注入均已从本方案移除（保留只会让评审面对一整套死代码）；
- 无备份是可接受的取舍：session 内容仍在共享 db（跨作业可见），D7 的 90 天归档作为误删兜底，用户如需长期/异地留存应自行拷出。

> 历史：本方案最初计划把 db 迁到 workfs2 以获得备份，并为此设计/实现了 copy-verify-swap 搬迁助手（含跨 app/跨节点锁、daemon 生命周期锁探测、可续跑标记、摘要 + quick_check 校验，历经三轮 bot 评审打磨）。配额实测（4GB vs 5GB）推翻了该前提，故整体撤除——未来需要时可作为独立 MR 重新引入。

### D3 大体积子目录拆分（run.sh + overlay env）

```bash
# 仅当站点提供 per-user scratch 根（已解析绝对路径）时启用；未设置保持上游现状
if [ -n "${INK_SCRATCH_ROOT:-}" ]; then
    if fs_cmd mkdir -p "${INK_SCRATCH_ROOT}/openchamber-chats" "${INK_SCRATCH_ROOT}/opencode-tool-output"; then
        export OPENCHAMBER_CHATS_DIR="${OPENCHAMBER_CHATS_DIR:-${INK_SCRATCH_ROOT}/openchamber-chats}"
        db_dir="$(fs_cmd readlink -f ~/.local/share/opencode)"
        if [ -n "$db_dir" ] && [ -d "$db_dir" ]; then
            if [ -d "$db_dir/tool-output" ] && [ ! -L "$db_dir/tool-output" ]; then
                # 先拷入 scratch，成功才删原目录（ln 到仍存在的真实目录会把链接嵌进去）
                if timeout 300 cp -a "$db_dir/tool-output/." "${INK_SCRATCH_ROOT}/opencode-tool-output/"; then
                    timeout 120 rm -rf "$db_dir/tool-output"
                else
                    echo "[INK] WARN: tool-output 拷贝失败，保留在原处" >&2
                fi
            fi
            # 仅当原路径确实已消失（或本就是链接）才建软链
            [ -e "$db_dir/tool-output" ] || [ -L "$db_dir/tool-output" ] \
                || timeout 30 ln -s "${INK_SCRATCH_ROOT}/opencode-tool-output" "$db_dir/tool-output"
        fi
    else
        echo "[INK] WARN: scratch 根不可达，保持未拆分布局" >&2
    fi
fi
```

- `INK_SCRATCH_ROOT` 由提交侧从 `service.scratch_root`（支持 `{experiment_group_lower}/{username}` 模板）渲染后经 job env 注入（Q1 已决）；jobtype env 显式值仍可覆盖。
- chats 拆分对已有会话无影响：openchamber-web 的 `chatRoots` 同时扫描 DATA_DIR/chats 与 `OPENCHAMBER_CHATS_DIR`（2.2.0/2.3.2 均确认）。
- 上游未配置时整块跳过（零配置可用）。

### D4 单实例强制（flock 实例锁，run.sh）

```bash
# 在拉起 daemon 前；per-user 固定目录下所有作业共用同一路径
exec 9>"${OPENCHAMBER_DATA_DIR}/.instance.lock"
if [ "${INK_DATA_DIR_DERIVED:-0}" = "1" ] || [ "${INK_DATA_DIR_PINNED:-0}" = "1" ]; then
    flock -w 30 9 || { echo "[INK] ERROR: another openchamber job holds the instance lock"; exit 1; }
elif ! flock -n 9; then
    echo "[INK] WARN: could not take the per-job instance lock; continuing on submit-side dedup"
fi
```

兜底新旧交替窗口。**语义分级**：共享 per-user（或用户自配）目录下锁是承重的——先等 30s（容忍并发重定位之类秒级占用）再拒启；`per-job` 模式锁只保护各作业自己的目录，取不到只 WARN 并回退提交侧 dedup。`flock` 命令缺失时同样只 WARN。flock 在 lustre/workfs2 上可用（需在 fastink-test 验证）。

### D5 preflight 扩展（overlay + run.sh，复用现有机制）

run.sh 在调用 preflight 前把本作业实际使用的目录**追加**进去：

```bash
export OPENCHAMBER_PREFLIGHT_EXTRA_PATHS="${OPENCHAMBER_PREFLIGHT_EXTRA_PATHS:+${OPENCHAMBER_PREFLIGHT_EXTRA_PATHS}:}${OPENCHAMBER_DATA_DIR}${OPENCHAMBER_CHATS_DIR:+:${OPENCHAMBER_CHATS_DIR}}"
```

（顺序调整：D1 的目录计算移到 preflight 之前。）**实现时加强**：extra paths 现在对 min-free 拦截，且对尚不存在的路径自动上溯到最近的已存在祖先再探测；**分层处理**——`INK_SCRATCH_ROOT` 之下的可再生路径（chats/tool-output）空间不足只告警，其余（含 workfs2 上的 DATA_DIR）直接拦截。

### D6 空间管理闭环（新维护脚本 + run.sh 挂接）

新增 `openchamber-maintenance.sh`（fastink-dev `cvmfs/ink/scripts/`），run.sh 在拉起 daemon 前、拿到实例锁之后调用（此时 **openchamber 侧**独占 db；登录节点直跑的 opencode CLI 不在该锁的约束内——见 §2 非目标，故此处不能声称绝对独占）：

```bash
OPENCHAMBER_MAINTENANCE="${OPENCHAMBER_MAINTENANCE:-}"   # 上游默认空 = 跳过
timeout "${INK_MAINTENANCE_BUDGET_SEC:-600}" "${OPENCHAMBER_MAINTENANCE}" || echo "WARN rc=$?; continuing"
```

脚本内容（全部条件触发；**所有 FS 操作带超时**，任一步超时本轮跳过并写日志、不影响启动，见 D8）：

0. **路径解析**：`db_dir="$(readlink -f ~/.local/share/opencode)"`——一切操作以解析结果为准，不假设规范路径。
1. **WAL 检查点 + 压缩**：`db-wal > 100MB` 或 7 天未 vacuum（`.last_vacuum` 戳）时 `PRAGMA wal_checkpoint(TRUNCATE); VACUUM;`。VACUUM 是 sqlite 单事务，中断安全；本轮超时则下次再试。**失败退避**：一次失败/超时后写 `.last_vacuum_failed`，3 天内不再尝试 VACUUM（多 GB 库在慢盘上否则每次作业启动空耗到超时）；成功则清除失败戳。
2. **旧 session 归档 + 删除**（默认保留 90 天）：
   - 导出 JSON.gz 到 `${INK_SCRATCH_ROOT}/session-archive/YYYY-MM/<id>-<slug>.json.gz`；
   - **归档查询报错或写盘失败即跳过该会话的删除**（宁可不删，绝不产出空归档后照删）；
   - 级联已按真实 schema 核实：所有引用 session/session_v2 的 FK 均为 `ON DELETE CASCADE`（session_v2←session_message/instruction_entry/instruction_state/session_pending/session_inbox；session←message/todo/session_share；message←part），删除不会因 FK 约束回滚；v2 的 parts 内嵌在 `session_message.data.content`（随消息导出），legacy 链的 message/part 单独导出；
   - 按 `sqlite_master` 检出的表存在性执行删除，单表历史库也能清理；逐会话事务，可续跑；无归档目录则完全不删。
3. **残留物清理**：`logs/` 保留最近 30 天（不被会话引用）；`tool-output/`、`snapshot/` 与**会话保留期同窗**（默认 90 天）——它们可能被保留期内的会话引用（rewind 快照、消息里的工具输出），30 天窗口会误删。**chats/ 首版不清理**——worktree 目录 mtime 不随内部写入更新，误删活跃会话的 worktree 会破坏续聊；chats 本就在大容量 scratch 层。
4. **通知**：清理前 7 天在作业启动 banner 提示（`>90-7` 天会话数）。归档目录自身保留 1 年。

### D7 归档格式

每个被清理的 session 导出一个自包含文件：

```
/scratchfs2/{group}/{user}/.ink/session-archive/YYYY-MM/<session_id>-<slug>.json.gz
```

```json
{
  "format": "ink.opencode.session-archive.v1",
  "archived_at": 1759..., 
  "session": { "…": "session_v2 整行" },
  "messages": [ { "…": "session_message 整行，按 seq 排序" } ],
  "legacy_messages": [ "…" ],
  "legacy_parts": [ "…" ]
}
```

- 归档定位是**人工可读的兜底**（丢了能找 dev 手工恢复），不做自动回灌 UI；
- **覆盖范围（设计决定）**：session 行 + 消息内容（v2 的 `session_message`，parts 内嵌于 `data.content`；旧链 `message`+`part`）。级联删除的瞬时工作态**不入归档**：`instruction_entry`、`instruction_state`、`session_pending`、`session_inbox`、`todo`、`session_share`——离开活会话即无意义或可再生。归档保证的是会话内容，不是每会话工作态；
- **取舍**：归档与活库同在 scratchfs2，一旦该层丢失两者同失（本方案不再把 db 迁到备份层，见 D2）。归档的价值在防**误删**，不在防盘损；需长期留存请自行拷出。

### D8 文件系统 hang 防护（横切：所有 FS 操作必须带超时）

lustre/scratchfs2/AFS 病态时 `du`、`stat`、`grep`、`sqlite3` 会**无限期阻塞**——没超时的检查会把瞬态故障放大成"作业启动永久卡死"。统一封装：`fs_cmd() { timeout "${INK_FS_CMD_TIMEOUT:-10}" "$@"; }`（run.sh / preflight / maintenance 共用；`shell.sh` 的断链守卫也包了超时）。

| 位置 | 操作 | 超时 | 超时后行为 |
|---|---|---|---|
| preflight 单项（quota/stat/读文件/NSS） | df / quota / stat / grep / id / getent | 10s（脚本内总预算 60s；run.sh 外层再包 180s） | 关键项（db 可写/非 AFS）→ die；建议项（quota% 水位、scratch 层空间）→ WARN 继续 |
| D6 维护 | wal_checkpoint / VACUUM | 600s（另加脚本外层预算，默认 600s） | 中断安全（sqlite 事务）；本轮跳过，下次再试 |
| D6 维护 | 归档导出 / 清理 find / rm | 单项 300s | 本轮跳过 |
| D1.1 播种 | 扫描 / du / cp / rm | 扫描 10s、拷贝按体积估算 | 探测超时不写标记、下次重试；拷贝失败 WARN 后空白启动 |

原则：**宁可跳过一次检查/维护，不可挂住一次启动**。所有超时事件写日志。

### 开源缺省可用性审计（零配置 = 行为与现状一致）

| 机制 | 上游默认值 | 缺省行为 |
|---|---|---|
| `OPENCHAMBER_DATA_MODE` | `per-job` | DATA_DIR 仍是每作业目录，与今天一致 |
| `code_datadir_root`（base 模板） | `/home/{username}`（既有默认值，本方案不改） | AFS 用户懒迁移逻辑不变 |
| `scratch_root` | `""` | D3 整块跳过：chats 留 DATA_DIR、tool-output 留 db 目录 |
| D4 flock | 共享目录下承重（失败即拒启）；per-job 目录仅兜底 | 共享 per-user 目录取不到锁（并发或 FS 不支持 flock）→ `exit 1`；per-job 目录取不到只 WARN + 回退 dedup |
| `OPENCHAMBER_PREFLIGHT_EXTRA_PATHS` | 恒非空 | DATA_DIR 会被追加进去做配额检查（站点未配置也一样）——**有意的行为变化** |
| `OPENCHAMBER_MAINTENANCE` | 空 | D6 挂接点跳过 |
| D8 `fs_cmd` 超时封装 | `INK_FS_CMD_TIMEOUT=10` | 只包裹新增逻辑；上游无新检查项时为零影响 |
| D1.1 播种 | 仅 `per-user` 且目录由本作业推导时触发 | per-job 缺省 / 用户自配 / 已播种，三种情况均不触发 |
| DATA_DIR `chmod 700` | 仅对"本脚本拥有"的目录 | 站点经 env hook 自配的目录**不改权限**（`INK_DATA_DIR_PINNED`） |
| DATA_DIR `mkdir` 失败 | `exit 1` | 有意变化：无法创建数据目录时快速失败并给明确原因 |

即：开源用户不做任何配置修改，**除上表标注的有意变化外**行为与升级前逐项等价。IHEP 的全部差异都在 fastink-dev overlay 的 config/env 里。

> 已知的上游泛化缺口（既有问题，不在本方案范围）：`code_datadir.py` 的 `AFS_PREFIX="/afs"` 硬编码。本方案新增的 `scratch_root` 是配置驱动的，不加剧该缺口。

## 5. 兼容性与迁移计划

| 对象 | 影响 | 处理 |
|---|---|---|
| 在跑的作业 | 无 | 旧作业继续用每作业目录，自然到期退出 |
| **历史作业目录里的 openchamber-data** | 固定目录首次为空：注册表/定时任务从空白开始，用户需重新打开一次项目目录 | **D1.1 首次启动播种**（Q5 已决：copy-only，源目录绝不修改；用户自配固定目录的跳过；`.seeded`+`settings.json` 双守卫） |
| 手工迁移过 opencode 数据的用户（介质合格、路径非规范） | 无需搬迁 | 保持原样（Q7 已决：不归一）；工具链按 `readlink -f` 解析结果工作 |
| 其他站点（上游） | 默认行为不变 | D1 默认 per-job；`scratch_root` 默认空；D6 脚本默认不存在即跳过 |
| 回滚 | 配置/env 还原即可 | 固定目录残留无害 |

## 6. 风险与缓解

| 风险 | 缓解 |
|---|---|
| opencode.db 无备份且持续增长（~4GB/用户） | db 留 scratchfs2（大容量）；D6 条件 vacuum + 90 天保留把体积压在配额内；归档作误删兜底。workfs2 装不下是选择不迁移的原因（D2） |
| workfs2 配额被 chats/tool-output/logs 耗尽 | D3 把 chats/tool-output 下沉 scratchfs2；D5 preflight 检查 DATA_DIR 配额；D6 清理 logs |
| flock 在 lustre 上的语义差异 | fastink-test 实测；共享目录取不到锁拒启，per-job 降级 |
| VACUUM 期间作业被杀 → db 损坏窗口 | 在实例锁后、daemon 前执行；sqlite 事务保证原子性 |
| 播种/清理误删用户内容 | 播种全程 copy-only 且源目录不修改；清理只删 >90 天且归档成功的 session；`.seeded`+`settings.json` 双守卫 |
| lustre/AFS 病态时 `du`/`stat`/`sqlite3` 无限期阻塞 → 启动永久卡死 | D8：所有 FS 操作带超时，超时分级处置；宁可跳过不可挂死 |
| 旧作业目录被清理（播种只取最新一份，更早历史只存在于旧目录） | INK 代码无作业目录清理机制（实证：7 周前目录仍存活）；会话内容与项目会话不受影响，仅 managed chats 的 worktree 随目录消失。残余风险 = workfs2 管理侧清理策略（代码之外）→ 建议不对含 `openchamber-data` 的旧作业目录做主动清理 |

## 7. MR 拆分与实施顺序

**实现状态（2026-10-16）**：两 MR 代码已完成并 force-push；经 ink-review-bot 多轮评审，全部发现已处置。

| # | 仓库 | 内容 | 状态 |
|---|---|---|---|
| MR !182 | fastink-code | run.sh：D1（OPENCHAMBER_DATA_MODE）+ D1.1（copy-only 播种）+ D3（INK_SCRATCH_ROOT 守卫的拆分）+ D4（flock）+ D5（EXTRA_PATHS 追加）+ D6 挂接 + D8（fs_cmd 超时封装）；shell.sh 断链守卫加超时；`code_datadir.py`：`render_storage_env()`（scratch 渲染）+ root 尾斜杠归一；utils.py：向 openchamber 作业注入 `INK_SCRATCH_ROOT`；base 模板补 `scratch_root`（零影响缺省）；单测 | ✅ 已实现 |
| MR !53 | fastink-dev | config.ihep.yml：`service.scratch_root` + `jobtype.openchamber.env.{OPENCHAMBER_DATA_MODE,OPENCHAMBER_MAINTENANCE}`（`code_datadir_root` 维持 scratchfs2）；新增 `cvmfs/ink/scripts/openchamber-maintenance.sh`；`openchamber-preflight.sh` 补全 timeout + 分层 extra paths | ✅ 已实现 |
| 依赖顺序 | — | **fastink-code !182 先合并**（!53 的配置键与维护脚本调用点都在 !182）；CVMFS 人工发布两个脚本 | 待人工 |
| 真机验证 | fastink-test | 固定目录落点、跨作业历史可见、flock 行为、preflight extra paths、vacuum/归档 | 待部署后 |

**已完成的沙箱测试**（本机合成环境，非真实作业）：迁移类测试已随 D2 撤销删除；现存——播种三用例（无候选→写标记 / 有候选→播种 / 扫描超时→不写标记）；维护脚本 DRYRUN 与真实执行（含 legacy message/part 导出、级联干净、单表库可清理、无 tmp 残留）；preflight 端到端 rc=0 与 extra paths 上溯探测；`bash -n` / `py_compile` / deploy 渲染测试全过。

## 8. 决策记录（原开放问题；2026-10-16 全部拍板）

- **Q1**：scratchfs2 根路径如何传入 run.sh——**已决（按推荐实现）**：新配置项 `service.scratch_root`（支持 `{experiment_group_lower}/{username}` 模板），由提交侧 `render_storage_env()` 渲染后经 job env `INK_SCRATCH_ROOT` 注入；jobtype env 显式值仍可覆盖。
- ~~**Q2**：存量用户 db 从 scratchfs2→workfs2 的再迁移在哪做~~ **已撤销**：db 不再迁移（见 D2，配额实测 4GB vs 5GB），相关机制整体移除。
- ~~**Q3**：固定 DATA_DIR 后 `run/` 与 `logs/` 是否拆回每作业目录~~ **已决**：按倾向执行——`run/` 留 DATA_DIR 依赖 openchamber 自清 + D4 实例锁；`logs/` 留 DATA_DIR 靠 D6 保留策略（30 天）。
- ~~**Q4**：session 保留期默认值~~ **已决**：按默认 90 天（`INK_SESSION_KEEP_DAYS=90`）；归档保留 1 年（`INK_SESSION_ARCHIVE_DAYS=365`）。
- ~~**Q5**：是否接受"新固定目录首次为空"~~ **已决：不接受，采纳迁移**——三条硬保证（D1.1）：copy-only 绝不删源、用户自配固定目录的跳过、`.seeded` 一次性。播种失败不阻塞启动。
- ~~**Q6**：opencode CLI 用户落 workfs2 的配额影响~~ **已撤销/不适用**：db 不迁 workfs2。
- ~~**Q7**：介质合格但路径非规范的手工迁移用户是否归一~~ **已决：不归一**，尊重用户手工配置；工具链按解析路径工作（维护脚本/preflight 均 `readlink -f` 后操作）。
- ~~**Q8**：preflight 是否泛化到 vnc/vscode/enode/rootbrowse~~ **已决：暂不考虑泛化**。分析存档：现状只有 openchamber 有 preflight（opencode 仅断链守卫）；`.bashrc` 自引用死循环在提交侧表现为 `su -` 挂起 30s 后笼统报 "submit job timeout"，配额满因 shell.sh 只有 `set -u` 无 `set -e` 表现为"作业 running 但功能坏"。若日后重启：零成本检查（配额/断链）保留拦截式，`.bashrc` 类走诊断（静态扫描写作业日志）而非阻塞探针；错误呈现靠 History Jobs 被动可查（不做主动推送）。建议作独立 MR。

## 9. 业务评审：与前端/后端对 openchamber 的产品目标对齐

产品目标取自两份长会话设计文档（`openchamber-long-lived-session.md` 与 inkfront-code `openchamber-long-lived-session-frontend.md`）：

- **资源侧**：作业生死由"是否真的被使用"决定（空闲 24h 回收、硬上限 7 天），而不是固定时长；
- **会话侧**：独立会话网关（浏览器 `sid` cookie + Redis 7 天滑动 TTL），与主站 24h JWT 解耦，"作业活着会话就在"；网关用 per-job 密码代换 OpenChamber 自己的 UI JWT（`oc_ui_session`）；
- **安全侧**：会话可独立吊销；不做前端存密码类方案。

### 9.1 对齐情况

| 产品目标 | 本方案影响 |
|---|---|
| 空闲回收不丢状态 | ✅ 固定目录 + copy-only 播种：回收对用户透明，"资源效率"不再以用户体验为代价 |
| 会话可用性 | ✅ `jwt-secret`/`ui-passkeys` 跨作业持久 → 新作业不再使浏览器里的 `oc_ui_session`/passkey 登录态整体失效（缓解网关文档 §5.8 在**跨作业**场景的重登问题；作业内 JWT 过期问题不在本方案范围）；per-job 密码与会话网关（Redis sid→job_id/host/port/passwd）完全不变 |
| 资源效率 | ✅ chats/tool-output 下沉 scratch；维护闭环压制 db 增长；启动开销有界（preflight 180s + 维护 600s 外层预算 + vacuum 失败 3 天退避） |
| 安全可控 | ✅ 共享目录 700（仅本脚本拥有的目录）、实例锁、passkeys/secret 不再随作业孤儿化（吊销路径不变） |
| 前端契约 | ✅ 前端对 openchamber 数据目录**零硬编码**（grep 无命中）；连接仍走 `app_login.info`（per-job）与网关，均不受影响 |

### 9.2 新引入的业务风险与取舍

1. **test/prod 双渠道共享同一固定目录**：目录由用户路径推导、与部署渠道无关 → 同一用户的 test 部署与 prod 部署作业共享 state 并被实例锁互斥（第二个拒启，报错可读）。取舍：两个 daemon 共写同一 state 本是损坏风险，互斥是正确方向；若未来需渠道隔离，可在目录名加渠道后缀。
2. **vacuum 重试风暴**（已修）：首启/失败后每次启动尝试 VACUUM（上限 600s），多 GB 库在慢盘上会每次空耗 → 现加失败后 3 天退避。
3. **db 无备份仍是残余风险**：scratchfs2 层丢失 = 历史与归档同失（同一丢失域，归档不防盘损）。取舍：配额所限（4GB vs 5GB）接受；若业务后续要"防盘损"，需独立的定期导出到备份层，不在本方案范围。
4. **灰度路径自然成立**：`OPENCHAMBER_DATA_MODE`/`OPENCHAMBER_MAINTENANCE` 仅在 config.ihep.yml（测试 overlay）；生产 overlay 未加键前，prod 作业维持 per-job 现状。注意 CVMFS 上的 preflight 加固一经发布对生产即生效（与 !182 无关，已在 !53 说明）。
5. **用户感知与文档**：cchelp 用户指南（#637）与前端 "Before you use Openchamber" 弹窗宜补（避免内部术语）："自本次升级起，你的项目列表、会话分组与历史会话会在每次新作业中自动保留。升级之前产生的更早记录仍保存在各自的旧作业目录中，可在 History Jobs 里查看；清理旧作业目录前请留意其中可能仍有你需要的早期内容。"
6. **History Jobs 自删**：前端允许删除旧作业目录；播种后删除旧目录仅丢失"最新一次作业之前"的 chats。需在用户文档说明。
