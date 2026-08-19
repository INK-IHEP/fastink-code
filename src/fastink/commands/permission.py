import argparse
import json

from sqlalchemy.exc import DatabaseError, IntegrityError, NoResultFound

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
                "docs": "Delete a permission and all associated user/group permissions",
                "namespace": self.delete_namespace,
            },
            "add_group_permission": {
                "call": self.add_group_permission,
                "docs": "Map a Linux group to a permission",
                "namespace": self.add_group_permission_namespace,
            },
            "delete_group_permission": {
                "call": self.delete_group_permission,
                "docs": "Remove a Linux group-to-permission mapping",
                "namespace": self.delete_group_permission_namespace,
            },
            "list_group_permissions": {
                "call": self.list_group_permissions,
                "docs": "List all group-permission mappings",
                "namespace": self.list_group_permissions_namespace,
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

    # —— Group permission management ——

    def add_group_permission_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--group",
            required=True,
            help="Linux group name",
        )
        parser.add_argument(
            "--permission",
            required=True,
            help="Permission name",
        )

    def delete_group_permission_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--group",
            required=True,
            help="Linux group name",
        )
        parser.add_argument(
            "--permission",
            required=True,
            help="Permission name",
        )

    def list_group_permissions_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--group",
            required=False,
            help="Filter by Linux group name",
        )

    def add_group_permission(self):
        try:
            permission_id = common.get_permission(
                permission=self.args.permission
            )["id"]
        except NoResultFound:
            print(f"Permission '{self.args.permission}' does not exist!")
            return
        try:
            common.add_group_permission(
                group_name=self.args.group, permission_id=permission_id
            )
            print(
                f"Group '{self.args.group}' -> '{self.args.permission}' "
                f"mapping added!"
            )
        except IntegrityError:
            print(
                f"Group '{self.args.group}' -> '{self.args.permission}' "
                f"already exists!"
            )

    def delete_group_permission(self):
        try:
            permission_id = common.get_permission(
                permission=self.args.permission
            )["id"]
        except NoResultFound:
            print(f"Permission '{self.args.permission}' does not exist!")
            return
        try:
            common.delete_group_permission(
                group_name=self.args.group, permission_id=permission_id
            )
            print(
                f"Group '{self.args.group}' -> '{self.args.permission}' "
                f"mapping deleted!"
            )
        except DatabaseError as e:
            print(f"Failed to delete group permission mapping: {e}")

    def list_group_permissions(self):
        try:
            mappings = common.get_all_group_permissions()
            if self.args.group:
                mappings = [
                    m for m in mappings
                    if m["group_name"] == self.args.group
                ]
            if not mappings:
                print("No group-permission mappings found")
                return
            print("Group-Permission mappings:")
            print(json.dumps(mappings, default=convert_to_str, indent=4))
        except Exception as e:
            print(f"Failed to list group permissions: {e}")
