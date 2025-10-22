from fastapi import Header, HTTPException
import subprocess

from src.computing.tools.resources_utils import sub_command


async def get_user_info(
    uid: int = Header(..., description="用户的Slurm UID"),
    email: str = Header(..., description="用户邮箱")
):
    try:
        # 通过系统命令获取用户名
        command_get_user = f"getent passwd {uid}"
        result_user = await sub_command(command_get_user, timeoutsec=2, errinfo="getent err", tminfo="getent timeout")
        # result_user = subprocess.run(command_get_user, shell=True, capture_output=True, text=True)

        # if result_user.returncode != 0 or not result_user.stdout:
        #     raise HTTPException(status_code=404, detail="User not found with given UID")

        # 提取用户名
        # user_info = result_user.stdout.strip().split(":")
        user_info = result_user.strip().split(":")
        username = user_info[0]  # 获取用户名

        # 使用 sacctmgr 命令查询用户相关信息
        command_sacctmgr = f"sacctmgr show assoc where user={username} format=Account,QOS,Partition -P"
        result_sacctmgr = await sub_command(command_sacctmgr, timeoutsec=2, errinfo="sacctmgr err", tminfo="sacctmgr timeout")
        # result_sacctmgr = subprocess.run(command_sacctmgr, shell=True, capture_output=True, text=True)

        # if result_sacctmgr.returncode != 0:
        #     raise HTTPException(status_code=500, detail=f"Failed to retrieve user info: {result_sacctmgr.stderr}")

        # 解析结果
        output_lines = result_sacctmgr.strip().split("\n")
        if len(output_lines) < 2:
            raise HTTPException(status_code=404, detail="User info not found")

        # 提取用户信息
        user_info = output_lines[1].split("|")
        account, qos, partition = user_info

        # 返回用户信息
        return {
            "status": 200,
            "msg": "请求成功",
            "data": {
                "account": account,
                "qos": qos,
                "partition": partition
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error occurred: {str(e)}")
