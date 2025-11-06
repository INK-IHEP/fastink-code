#!/usr/bin/env python3
from sqlalchemy.exc import IntegrityError
from src.auth.common import (
    add_user,
    get_user,
    update_user,
    add_permission,
    add_user_permission,
    get_permission,
)
from src.auth.krb5 import get_krb5, create_krb5


def prepare():
    users = [
        {"username": "jlovemercury", "email": "jlovemercury@hotmail.com", "uid": 23611},
        {"username": "changzhi", "email": "changzhi@ihep.ac.cn", "uid": 10894},
        {"username": "dingxf", "email": "dingxf@ihep.ac.cn"},
        {"username": "yangliumatrix", "email": "yangliu_matrix@petalmail.com"},
        {"username": "lihaibo", "email": "lihaibo@ihep.ac.cn"},
        {"username": "common", "email": "common@ihep.ac.cn"},
        {"username": "wenlj", "email": "wenlj@ihep.ac.cn”"},
    ]

    for user in users:
        try:
            add_user(**user)
            print(f"Successfully adding user {user['username']}")
        except:
            print(f"error occured when adding user {user['username']}")
            pass

    try:
        add_permission("cpu")
    except:
        pass
    cpu_id = get_permission("cpu")["id"]

    try:
        add_permission("gpu")
    except:
        pass
    gpu_id = get_permission("gpu")["id"]

    try:
        add_permission("AlmaLinux9")
        add_permission("CentOS7")
    except:
        pass

    for user in users:
        print(user["username"])
        user_id = get_user(username=user["username"])["id"]
        print(user_id)
        try:
            print(f"user: {user['username']}, user_id: {user_id}")
            add_user_permission(user_id, cpu_id)
        except Exception as err:
            print(
                f"error occured when adding permission {cpu_id} for user {user['username']}"
            )
            print(err)
            pass

    gpuusers=[
        "jlovemercury",
        "yangliumatrix"
    ]
    for gpuuser in gpuusers:
        user_id=get_user(username=gpuuser)["id"]
        try:
            print(f"user: {gpuuser},user_id:{user_id}")
            add_user_permission(user_id=user_id, permission_id=gpu_id)
        except Exception as err:
            print(
                f"error occured when adding permission {gpu_id} for user {gpuuser}"
            )
            print(err)
            pass


def get_token(username="gb062023101401", password="IHEP@123ok"):
    # Create krb5 token for guocq
    create_krb5(username=username, password=password)

    # Get token method
    token = get_krb5(username=username)
    print("token: ", token)

    return token


if __name__ == "__main__":
    prepare()
#    token = get_token()
