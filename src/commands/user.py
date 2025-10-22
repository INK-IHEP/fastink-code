import argparse
import json

from src.auth import krb5
from src.auth import permission
from src.auth import user
from src.commands.basecommand import CommandBase, OperationDict
from src.common.utils import convert_to_str


class User(CommandBase):

    def module_help(self) -> str:
        return "Users managerment"

    def usage_example(self) -> list[str]:
        return [
            "ink user list",
            "ink user list --username=test",
            "ink user add --username=test --email=test@test.com --uid=12345",
            "ink user delete --username=test --email=test@test.com --uid=12345",
            "ink user update --username=test --new-username=test2 --new-email=test2@test.com --new-uid=12346",
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
        parser.add_argument(
            "--email",
            required=False,
            help="Email of the user",
        )

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--username",
            required=False,
            help="Username of the user",
        )
        parser.add_argument(
            "--email",
            required=False,
            help="Email of the user",
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
            "--email",
            required=False,
            help="Email of the user",
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
            "--email",
            required=False,
            help="Email of the user",
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
            "--new-email",
            required=False,
            help="New email of the user",
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
            "--email",
            required=False,
            help="Email of the user",
        )
        parser.add_argument(
            "--uid",
            required=False,
            help="UID of the user",
        )

    def list_users(self) -> None:
        result = user.list_users()
        if self.args.username:
            result = [user for user in result if user["username"] == self.args.username]
        if self.args.email:
            result = [user for user in result if user["email"] == self.args.email]
        if not result:
            print(f"User {self.args.username} not found!")
            return
        print("Listing users...")
        print(json.dumps(result, default=convert_to_str, indent=4))

    def add_user(self):
        result = user.add_user(
            username=self.args.username,
            email=self.args.email,
            uid=self.args.uid,
        )
        if result:
            print(f"{self.args.username} added!")
        else:
            print(f"{self.args.username} adding failed!")

    def update_user(self):
        result = user.update_user(
            username=self.args.username,
            email=self.args.email,
            uid=self.args.uid,
            new_username=self.args.new_username,
            new_email=self.args.new_email,
            new_uid=self.args.new_uid,
        )
        if result:
            print(f"{self.args.username} updated!")
        else:
            print(f"{self.args.username} updating failed!")

    def delete_user(self):
        result = user.delete_user(
            username=self.args.username,
            email=self.args.email,
            uid=self.args.uid,
        )
        if result:
            print(f"{self.args.username} deleted!")
        else:
            print(f"{self.args.username} deleting failed!")

    def get_user_token(self):
        result = krb5.get_krb5(
            username=self.args.username, email=self.args.email, uid=self.args.uid
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
            "--email",
            required=False,
            help="Email of the user",
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
            username=self.args.username, email=self.args.email, uid=self.args.uid
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
