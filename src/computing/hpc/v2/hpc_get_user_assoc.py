from fastapi import Header, HTTPException
import subprocess

from src.computing.tools.resources_utils import sub_command, change_uid_to_username


async def get_user_assoc(uid):
    try:
        
        username = change_uid_to_username(uid)

        # get association info with sacctmgr command
        command_sacctmgr = f"sacctmgr show assoc where user={username} format=Account,QOS,Partition -P"
        result_sacctmgr = await sub_command(command_sacctmgr, timeoutsec=2, errinfo="sacctmgr err", tminfo="sacctmgr timeout")

        # parse result
        output_lines = result_sacctmgr.decode().strip().split("\n")
        if len(output_lines) < 2:
            raise HTTPException(status_code=404, detail="User info not found")

        # get user info
        user_info = output_lines[1].split("|")
        account, qos, partition = user_info

        return account, qos, partition

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error occurred: {str(e)}")
