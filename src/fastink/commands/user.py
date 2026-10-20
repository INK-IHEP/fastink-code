import argparse
import json

from fastink.auth.backends import krb5
from fastink.auth import permission
from fastink.auth import user
from fastink.auth.session import revoke_user_sessions_sync
from fastink.commands.basecommand import CommandBase, TopLevelCommand, OperationDict
from fastink.common.utils import convert_to_str



def _resolve_username(username, uid):
    """Return the target username, looking it up when only --uid was given."""
    if username:
        return username
    if uid is None:
        return None
    record = user.get_user(uid=uid)
    return record.get("username") if record else None

class User(TopLevelCommand):

    def module_help(self) -> str:
        return "Users managerment"

    def usage_example(self) -> list[str]:
        return [
            "ink user list",
            "ink user list --username=test",
            "ink user add --username=test --uid=12345",
            "ink user delete --username=test --uid=12345",
            "ink user update --username=test --new-username=test2 --new-uid=12346",
            "ink user token --username=test",
        ]

    def _operations(self) -> dict[str, OperationDict]:
        return {
            "add": {
                "call": self.add_user,
                "docs": "Add a new user",
                "namespace": self.add_namespace,
            },
            "list": {
                "call": self.list_users,
                "docs": "List all users",
                "namespace": self.list_namespace,
            },
            "delete": {
                "call": self.delete_user,
                "docs": "Delete an user",
                "namespace": self.delete_namespace,
            },
            "update": {
                "call": self.update_user,
                "docs": "Update an user information",
                "namespace": self.update_namespace,
            },
            "token": {
                "call": self.get_user_token,
                "docs": "Get a user token",
                "namespace": self.token_namespace,
            },
        }

    def implemented_subcommands(self) -> dict[str, type[CommandBase]]:
        return {"permission": Permission}

    def list_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
            help="UID of the user",
        )

    def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
            help="UID of the user",
        )

    def update_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
            help="UID of the user",
        )
        parser.add_argument(
            "--new-username",
            required=False,
            help="New username of the user",
        )
        parser.add_argument(
            "--new-uid",
            required=False,
            help="New UID of the user",
        )

    def token_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
            help="UID of the user",
        )

    def list_users(self) -> None:
        result = user.list_users()
        if self.args.username:
            result = [u for u in result if u["username"] == self.args.username]
        if not result:
            print(f"User {self.args.username} not found!")
            return
        print("Listing users...")
        print(json.dumps(result, default=convert_to_str, indent=4))

    def add_user(self):
        result = user.add_user(
            username=self.args.username,
            uid=self.args.uid,
        )
        if result:
            print(f"{self.args.username} added!")
        else:
            print(f"{self.args.username} adding failed!")

    def update_user(self):
        target = _resolve_username(self.args.username, self.args.uid)
        result = user.update_user(
            username=self.args.username,
            uid=self.args.uid,
            new_username=self.args.new_username,
            new_uid=self.args.new_uid,
        )
        if result:
            try:
                revoked = revoke_user_sessions_sync(target) if target else 0
                if self.args.new_username:
                    revoked += revoke_user_sessions_sync(self.args.new_username)
                print(f"{self.args.username} updated! ({revoked} sessions revoked)")
            except Exception as error:
                print(f"{self.args.username} updated, but session revocation failed: {error}")
        else:
            print(f"{self.args.username} updating failed!")

    def delete_user(self):
        target = _resolve_username(self.args.username, self.args.uid)
        result = user.delete_user(
            username=self.args.username,
            uid=self.args.uid,
        )
        if result:
            try:
                revoked = revoke_user_sessions_sync(target) if target else 0
                print(f"{self.args.username} deleted! ({revoked} sessions revoked)")
            except Exception as error:
                print(f"{self.args.username} deleted, but session revocation failed: {error}")
        else:
            print(f"{self.args.username} deleting failed!")

    def get_user_token(self):
        result = krb5.get_krb5(
            username=self.args.username, uid=self.args.uid
        )
        if result:
            print(f"{self.args.username} token is:\n{result}")
        else:
            print(f"{self.args.username} token not found!")


class Permission(CommandBase):
    def module_help(self) -> str:
        return "Permissions managerment"

    def usage_example(self) -> list[str]:
        return [
            "ink user permission list --username=test",
            "ink user permission add --username=test --permission=test",
            "ink user permission delete --username=test --permission=test",
        ]

    def implemented_subcommands(self) -> dict[str, type[CommandBase]]:
        return {}

    def _operations(self) -> dict[str, OperationDict]:
        return {
            "list": {
                "call": self.list_user_permissions,
                "docs": "List all users' permissions",
                "namespace": self.list_namespace,
            },
            "add": {
                "call": self.add_user_permission,
                "docs": "Add a new user permission",
                "namespace": self.add_namespace,
            },
            "delete": {
                "call": self.delete_user_permission,
                "docs": "Delete an user's permission",
                "namespace": self.delete_namespace,
            },
        }

    def list_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
        )

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=True,
            help="Username of the user",
        )
        parser.add_argument(
            "--permission",
            required=True,
            help="Permission of the user",
        )

    def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=True,
            help="Username of the user",
        )
        parser.add_argument(
            "--permission",
            required=True,
            help="Permission of the user",
        )

    def list_user_permissions(self):
        result = permission.query_user_permissions(
            username=self.args.username, uid=self.args.uid
        )
        print("Listing permissions...")
        print(json.dumps(result, default=convert_to_str, indent=4))

    def add_user_permission(self):
        result = permission.add_user_permission(
            username=self.args.username, permission=self.args.permission
        )
        if result:
            print(f"{self.args.permission} added!")
        else:
            print(f"{self.args.permission} adding failed!")

    def delete_user_permission(self):
        result = permission.delete_user_permission(
            username=self.args.username, permission=self.args.permission
        )
        if result:
            print(f"{self.args.permission} deleted!")
        else:
            print(f"{self.args.permission} deleting failed!")
