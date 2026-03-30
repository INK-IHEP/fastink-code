# FastINK Deploy

`ink-code/deploy/` 是 FastINK 的共享部署层，负责三件事：

- 定义可发布的官方镜像
- 提供共享的渲染内核和宿主机预检逻辑
- 提供开源用户可直接使用的交互式部署入口

它既服务开源用户，也服务 `fastink-dev` 这类站点 overlay。

## 目录结构

- `images/`
  官方镜像定义。当前包括 `server`、`cron`、`rootbrowse`。
- `lib/`
  共享部署内核，包括默认值、路径规划、宿主机预检、模板渲染。
- `templates/`
  分层模板。
  - `base/`：公共模板
  - `profiles/`：`minimal`、`full` profile overlay
  - `extras/`：可选能力 overlay，例如 `nginx`、`xrootd`
- `install.py`
  交互式 CLI，面向开源用户。
- `render_profile.py`
  非交互渲染入口，面向 CI 和 site overlay。
- `check_host.py`
  宿主机预检入口。

## 开源用户部署流程

在 `ink-code/` 根目录执行：

```bash
python deploy/install.py
```

或者：

```bash
./deploy/bin/fastink-deploy
```

实际流程是：

1. 执行宿主机预检。
   - 检查 `docker`
   - 检查 `docker compose`
   - 检查 `/cvmfs`
   - 预热 FastINK 会访问到的 `/cvmfs` 路径
2. 选择 profile。
   - `minimal`：FastINK 核心功能栈
   - `full`：在核心功能栈基础上，默认打开更多可选能力
3. 选择镜像来源。
   - 默认是 `pull`，直接拉官方镜像
   - 也可以显式选 `build`，在本地根据 `deploy/images/` 构建
4. 交互式填写运行参数。
   - 端口、目录、数据库口令、Redis 口令、可选服务开关等
5. 生成持久化部署目录 `.deploy/`。
6. 渲染模板并写入 `.deploy/`。
7. 执行 `docker build` 或 `docker pull`。
8. 执行 `docker compose up -d` 并轮询健康检查。

## Profile 与容器层级

当前 profile 语义是：

- `minimal`
  - `fastink-db`
  - `fastink-redis`
  - `fastink-server`
  - `fastink-redis-cron`
  - `fastink-rootbrowse`
- `full`
  - 继承完整 `minimal`
  - 再根据开关启用额外基础设施能力

当前可选 extra：

- `enable_nginx`
  - 增加 `fastink-nginx`
  - 由 nginx 提供 HTTPS 入口，再反代到 `fastink-server`
- `enable_xrootd`
  - 增加 `fastink-xrootd`

## `.deploy/` 里会生成什么

`.deploy/` 是开源用户的持久部署资产，建议随部署一起保存。换机器时，可以直接带走这份目录继续部署。

典型内容包括：

- `config.yml`
- `docker-compose.yml`
- `.env`
- `answers.json`
- `runtime/`
- `keys/`
- `plugins/`
- `preload/`
- `xrootd/`

这些文件中，真正建议手工维护的主要是：

- `answers.json`
- `plugins/`
- `preload/`
- `xrootd/` 下用户自己补充的运行时材料

生成产物例如 `config.yml`、`docker-compose.yml`，更适合通过重新执行 `install.py` 或复用 `answers.json` 来更新，而不是手工长期维护。

## 需要用户处理的运行时材料

### 1. SSH client key

deploy 会自动生成：

- `.deploy/keys/ssh-client/id_rsa`
- `.deploy/keys/ssh-client/id_rsa.pub`

这对 key 的用途是：

- `fastink-server` 访问 `rootbrowse`
- `fastink-server` 访问 condor、slurm、login 节点

`rootbrowse` 的 host key 由容器自己生成，不需要用户准备。

用户需要把 `.deploy/keys/ssh-client/id_rsa.pub` 部署到远端运行账号的 `authorized_keys`。

### 1.1 nginx TLS certificate

如果启用 `nginx`，deploy 会在 `.deploy/nginx/` 下维护：

- `default.conf`
- `cert.pem`
- `key.pem`

规则是：

- 如果用户在 installer 中提供已有证书和私钥，deploy 会把它们复制到 `.deploy/nginx/`
- 如果用户没有提供证书，deploy 会自动生成一对自签发证书，保证 nginx 至少可以提供 HTTPS 加密服务

当前通用 nginx extra 直接对外暴露的是 HTTPS 端口，并反代到容器内的 `fastink-server:8000`。

### 2. xrootd keytab

如果启用 `xrootd`，deploy 会在 `.deploy/xrootd/` 下准备：

- `sss.keytab`
- `krb5.keytab`

规则是：

- `sss.keytab`
  - 如果宿主机上有 `xrdsssadmin`，installer 会自动生成
  - 否则会保留占位文件，并提示用户执行：
    `xrdsssadmin -c .deploy/xrootd/sss.keytab -u xrootd`
- `krb5.keytab`
  - 只会保留占位路径
  - 需要 krb5 管理员提供并放到 `.deploy/xrootd/krb5.keytab`

### 3. Slurm 宿主机环境

如果 FastINK 需要访问 Slurm，当前要求是宿主机先准备好 Slurm client 环境。至少包括：

- `sbatch`
- `sacct`
- `scontrol`
- `scancel`
- 可挂载的 `munge` socket
- 正常工作的 `slurm.conf`

容器镜像负责提供容器内客户端命令，但宿主机自己的 Slurm 配置和认证仍然要提前准备好。

## 官方镜像与 yum 源

开源用户默认应直接拉官方镜像。

官方镜像在 `deploy/images/` 中定义，并在发布流程中构建。当前镜像构建会使用 IHEP 管理的 yum 源，其中 `slurm` 依赖来自 IHEP mirror。

如果你要自己 build 镜像，或者要在镜像里安装额外 RPM，请检查并按自己的环境修改：

- `deploy/images/repos/`

## 非交互渲染流程

`render_profile.py` 是给 CI 和 site overlay 用的，不是给普通用户直接维护 `.deploy/` 的主入口。

典型调用方式：

```bash
python deploy/render_profile.py \
  --profile full \
  --answers-file /path/to/render.answers.json \
  --output-dir /path/to/output \
  --config-overlay /path/to/site-config.yml
```

它的职责是：

1. 读取一份 answers 文件
2. 应用 `--set` 覆盖
3. 规划运行时目录
4. 渲染基础模板、profile overlay、extra overlay
5. 输出最终 `config.yml`、`docker-compose.yml`、`.env`

`fastink-dev` 就是通过这条链路消费 `deploy` 的。

## 需要改东西时去哪里改

按改动性质分：

- 改公共服务拓扑、公共 compose 结构：`templates/base/`
- 改 `minimal/full` 语义：`templates/profiles/`
- 改 `nginx/xrootd` 这类可选能力：`templates/extras/`
- 改默认值、默认镜像来源、profile 默认开关：`lib/defaults.py`
- 改运行目录结构：`lib/paths.py`
- 改宿主机预检和 `/cvmfs` 预热：`lib/host_runtime.py`
- 改模板渲染和 merge 行为：`lib/render.py`
- 改官方镜像内容：`images/`
- 改交互式安装体验：`install.py`
- 改 CI / 站点渲染入口：`render_profile.py`

原则只有一条：

- 通用部署语义留在 `ink-code/deploy`
- 站点差异不要回流到这里，而是放到站点 overlay 仓库中
