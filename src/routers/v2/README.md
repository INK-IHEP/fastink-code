# INK-API 规范

## 规则解释

### 1.在使用 HTTP 方法时，只能用`GET`和`POST`：

- 只有获取资源需要使用 GET 方法 @router.get

  - <span style="color:red">GET 方法只使用 Query 参数方式传参</span>

- 创建资源等使用 POST 方法 @router.post

  - <span style="color:red">POST 方法只使用 Body 参数方式传参</span>

- <span style="color:red">除了 Ink-Token 和 Ink-Username 以外，任何参数不得使用 header 方式传参</span>

### 2.所有的 API 接口必须以`<verb>_<none>`的方式命名，单词必须全部小写且使用下划线相连，除了专业术语之外，不能出现数字，只有在查询的时候才使用 get

- 例如：`token` 改为`/get_token`（连接使用下划线`_`）

- <span style="color:red">建议：用两阶接口，避免重复命名。第一阶为类型名，第二阶为操作接口，例如</span>

| 类型     | 功能                 | 原接口                 | 新接口                      | 点评                                                                                                                                                                   |测试|
| -------- | -------------------- | ---------------------- | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |--------------------------- |
| 文件系统 | 获取 home 目录       | `/home`                | `/fs/get_home`              | 是否用`fs`来命名类型可以商榷                                                                                                                                           |✔️|
| 文件系统 | 创建目录             | `/mkdir`               | `/fs/create_dir`             ||✔️
| 文件系统 | 列出文件             | `/list`                | `/fs/list_path`             ||✔️
| 文件系统 | 删除文件             | `/delete`              | `/fs/delete_path`           ||✔️
| 文件系统 | 上传文件             | `/upload`              | `/fs/upload_file`           ||✔️
| 文件系统 | 上传目录             | `/dirUpload`           | `/fs/upload_dir`            ||无
| 文件系统 | 下载文件             | `/download`            | `/fs/download_file`         ||✔️
| 文件系统 | 查看文件内容         | `/view`                | `/fs/view_file`             ||✔️
| 计算资源 | 连接作业             | `/connect_job`         | `/cr/connect_job`           | 是否用`cr`来命名类型可以商榷                                                                                                                                           |✔️|
| 计算资源 | 创建作业             | `/create_job`          | `/cr/create_job`            ||✔️
| 计算资源 | 创建作业             | 无          | `/cr/create_job_with_path`            |  v2新增接口，使用input file path和job file path来创建作业|HEPS用|
| 计算资源 | 删除作业             | `/delete_job`          | `/cr/delete_job`            ||❌
| 计算资源 | 获取作业输出         | `/get_job_output`      | `/cr/get_joboutput`         | 这里把 joboutput 连写，以满足`<verb>_<none>`格式                                                                                                                       |✔️|
| 计算资源 | 获取作业细节         | `/get_job_details`     | `/cr/get_jobdetails`        | 同上连写                                                                                                                                                               |✔️|
| 计算资源 | 获取集群信息         | `/get_system_info`     | `/cr/get_systeminfo`        | 同上连写                                                                                                                                                               |弃用|
| 计算资源 | 获取用户association信息         | `/get_use_info`        | `/cr/get_userassoc`          | 实际功能是获取用户的association信息，association在slurm中特指四元组（user，partition，account，qos）                                                                                                                                        |✔️|
| 计算资源 | 查询作业             | `/query_jobs`          | `/cr/query_jobs`            ||✔️
| 计算资源 | 查询系统作业         | `/query_system_jobs`   | `/cr/query_systemjobs`      | 同上连写                                                                                                                                                               |✔️|
| 计算资源 | 获取系统作业         | `/get_system_jobs`     | `/cr/get_systemjobs`        | 同上连写                                                                                                                                                               |✔️|
| 审计监控 | 获取 omat 每日作业   | `/get_omat_daily_jobs` | `/app/get_dailyjobs`    | 审计监控的业务建议单独放在一个类型里，而且我吐槽一下，这种和 omat 强绑定的接口，属于和 resource hub 思路不相符合的部分，开源困难，我建议放在中台做，或者做成插件形式。 |✔️|
| 审计监控 | 获取 omat 堆叠作业   | `/get_omat_stack_jobs` | `/app/get_stackjobs`    | 同上建议，这个 router 怎么把业务写进了 router？                                                                                                                        |✔️|
| 用户面板 | 获取用户当前作业状态 | `/getlaststate`        | `/ccs/get_cur_userjobs`    |                                                                                                                                                                        |✔️|
| 用户面板 | 获取用户历史作业状态 | `/getstatebytime`      | `/ccs/get_his_userjobs`    |                                                                                                                                                                        |✔️|
| 用户面板 | 获取用户当前存储状态 | `/getlastdisk`         | `/ccs/get_cur_userdisk`    |                                                                                                                                                                        |✔️|
| 用户面板 | 获取用户历史存储状态 | `/getdiskbytime`       | `/ccs/get_his_userdisk`    |                                                                                                                                                                        |✔️|
| 用户面板 | 确认用户名是否有效   | `/is-effective`        | `/ccs/verify_username`      | 可以弃用，直接调用公共 router                                                                                                                                          |✔️|
| 各种应用 | 查看集群作业情况     | `/get_job_statistics`  | `/app/get_job_statistics`   |                                                                                                                                                                        |✔️|
| 各种应用 | 获取 Token           | `/get_token`           | `/app/get_token`            | 可以弃用，直接调用公共 router                                                                                                                                          |❌弃用|
| 认证权限 | 生成 token           | `POST /token`          | `/auth/create_token`        |                                                                                                                                                                        |不用|
| 认证权限 | 获取 Token           | `GET /token`           | `/auth/get_token`           |                                                                                                                                                                        |✔️|
| 认证权限 | 获取用户权限         | `/permission`          | `/auth/get_permission` | 此为用户权限 router 定 义                                                                                                                                              |✔️|
| 认证权限 | 创建 INK 用户        | `POST /user`           | `/auth/create_user`         |                                                                                                                                                                        |✔️|
| 认证权限 | 获取 INK 用户信息    | `GET /get_user`        | `/auth/get_user`            |                                                                                                                                                                        |✔️|
| 各种服务 | 直接访问 root 文件   | `/service/open_root`   | `/service/access_rootfile`  ||✔️

### 3.header 中必须包含的值必须有`Ink-Username`值和`Ink-Token`值

<span style="color:red">这里如果有不需要认证的接口，可以告诉zhangxuantong@ihep.ac.cn，他会维护需要跳过认证的列表。已知不需要认证的接口有：</span>

- `/auth/get_token`
- `/auth/create_token`

### 4. 返回值必须为三段式，包括正常 return 和 exception 的格式都必须满足

HTTP 返回状态值不做要求。

```python
"""
{
    "status": str,
    "msg": str,
    "data": dict[str, Any]
}
"""
```

## 举例

```python
from src.routers.headers import get_username, get_token

@router.get("<routers>")
def router_function(
    username: str = Depends(get_username), # 获取username
    token: str = Depends(get_token), # 获取token，这个token很可能是无效的，所以大部分的接口都不需要这个token，可以不写这一行参数
    para1: Query(None)
    ...
):
    try:
        data = function(username, token) # 这里放你的业务函数
        reponse = {"status": str, "msg": str, "data": data} # 这里构造返回值
    except Exception as e:
        reponse = {"status": str, "msg": str(e), "data": None} # 这里构造异常的返回值
    return response

@router.post("<routers>")
def router_function(
    username: str = Depends(get_username), # 获取username，跳过获取token
    para1: str = Body(..., embed=True),
    para2: int = Body(..., embed=True) # embed=True 表示请求体必须为JSON对象并从中获取单个值，如果不使用embed=True，请求体必须是原始值（字符串、数字等）
    ...
)
```

### 5. 返回值代码定义

可参见： https://code.ihep.ac.cn/INK/ink/-/blob/main/src/routers/status.py?ref_type=heads

## 举例

```
from src.routers.status import InkStatus
print(InkStatus.SUCCESS) #打印状态码
response={"status": InkStatus.SUCCESS, "msg": "xxx", "data", {}} #构造返回值三段式
```

<span style="color:red">千万注意，router 只干如下 5 件事情，不要节外生枝：</span>

1. 传参：获取来自前端的参数，可简单处理一下参数
2. 调用业务函数：将参数传递给业务函数，并执行业务函数
3. 抓取异常：构造异常返回值
4. 构造正常返回值
5. 返回结果：返回结果给前端

<span style="color:red">所有业务逻辑请放到业务函数内，不要在 router 内写业务！</span>
