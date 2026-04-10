import argparse
import json

from fastink.auth import permission as perm_module
from fastink.auth import common
from fastink.commands.basecommand import TopLevelCommand, OperationDict
from fastink.common.utils import convert_to_str


class Permission(TopLevelCommand):

    def module_help(self) -> str:
        return "Permissions management"

    def usage_example(self) -> list[str]:
        return [
            "ink permission list",
            "ink permission add --permission=test_permission",
            "ink permission delete --permission=test_permission",
        ]

    def _operations(self) -> dict[str, OperationDict]:
        return {
            "add": {
                "call": self.add_permission,
                "docs": "Add a new permission",
                "namespace": self.add_namespace,
            },
            "list": {
                "call": self.list_permissions,
                "docs": "List all permissions",
            },
            "delete": {
                "call": self.delete_permission,
                "docs": "Delete a permission and all associated user permissions",
                "namespace": self.delete_namespace,
            },
        }

    def implemented_subcommands(self) -> dict[str, type["CommandBase"]]:
        return {}

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--permission",
            required=True,
            help="Name of the permission",
        )

    def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--permission",
            required=True,
            help="Name of the permission to delete",
        )

    def list_permissions(self) -> None:
        try:
            permissions = common.get_all_permissions()
            if not permissions:
                print("No permissions found")
                return
            print("Listing permissions...")
            print(json.dumps(permissions, default=convert_to_str, indent=4))
        except Exception as e:
            print(f"Failed to list permissions: {e}")

    def add_permission(self):
        result = perm_module.add_permission(permission=self.args.permission)
        if result:
            print(f"Permission '{self.args.permission}' added successfully!")
        else:
            print(f"Failed to add permission '{self.args.permission}'!")

    def delete_permission(self):
        result = perm_module.delete_permission(permission=self.args.permission)
        if result:
            print(f"Permission '{self.args.permission}' deleted successfully!")
        else:
            print(f"Failed to delete permission '{self.args.permission}'!")
