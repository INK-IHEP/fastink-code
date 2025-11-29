#!/usr/bin/env python3
from src.auth.permission import (
    has_permission,
    check_user_permission,
    query_user_permissions,
)
from src.auth.common import (
    add_permission,
    add_user,
    add_user_permission,
    get_permission,
    get_user,
)


# Use decorator, "user" parameter is mandetory in args. In this case the
# function's name must be a "permission_name" in database
@has_permission()
def read(user="permission_user"):
    return "I can do read"


# Use decorator, with specific user and permission in decorator's args
@has_permission(user="permission_user", permission="read")
def myfunc():
    return "I can do myfunc"


# Use function
def myfunc2():
    if check_user_permission("permission_user", "read"):
        return "I can do myfunc2"
    else:
        return "I cannot do myfunc2"


# Get token, you need to parse token flag to decorator
@has_permission(token_flag=True)
def write(user="permission_user", **kwargs):
    return f"I can get my token {kwargs['token']}"


# Add permissions for a certain user
def add_user_perm():
    # prepare a new permission by name
    add_permission("gpu")
    permission_id = get_permission("gpu")["id"]

    # prepare a new user by username and email
    add_user("testuser", "testuser@ihep.ac.cn")
    user_id = get_user(email="testuser@ihep.ac.cn")["id"]

    # add user with permission
    add_user_permission(user_id=user_id, permission_id=permission_id)

    # query if it is successfull added
    return query_user_permissions(email="testuser@ihep.ac.cn")


if __name__ == "__main__":
    # print(read(user="permission_user"))
    # print(myfunc())
    # print(myfunc2())
    # print(write(user="permission_user"))
    print(add_user_perm())
