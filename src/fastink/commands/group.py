import argparse
import json

from sqlalchemy.exc import DatabaseError, IntegrityError, NoResultFound

from fastink.auth import common
from fastink.commands.basecommand import CommandBase, TopLevelCommand, OperationDict
from fastink.common.utils import convert_to_str


class Group(TopLevelCommand):

    def module_help(self) -> str:
        return "Linux group management"

    def usage_example(self) -> list[str]:
        return [
            "ink group permission list",
            "ink group permission list --group=physics",
            "ink group permission add --group=physics --permission=CentOS7",
            "ink group permission delete --group=physics --permission=CentOS7",
        ]

    def _operations(self) -> dict[str, OperationDict]:
        return {}

    def implemented_subcommands(self) -> dict[str, type[CommandBase]]:
        return {"permission": Permission}


class Permission(CommandBase):
    def module_help(self) -> str:
        return "Linux group to permission mapping"

    def usage_example(self) -> list[str]:
        return [
            "ink group permission list",
            "ink group permission list --group=physics",
            "ink group permission add --group=physics --permission=CentOS7",
            "ink group permission delete --group=physics --permission=CentOS7",
        ]

    def implemented_subcommands(self) -> dict[str, type[CommandBase]]:
        return {}

    def _operations(self) -> dict[str, OperationDict]:
        return {
            "list": {
                "call": self.list_group_permissions,
                "docs": "List all group-permission mappings",
                "namespace": self.list_namespace,
            },
            "add": {
                "call": self.add_group_permission,
                "docs": "Map a Linux group to a permission",
                "namespace": self.add_namespace,
            },
            "delete": {
                "call": self.delete_group_permission,
                "docs": "Remove a Linux group-to-permission mapping",
                "namespace": self.delete_namespace,
            },
        }

    # —— Namespace ——

    def list_namespace(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--group",
            required=False,
            help="Filter by Linux group name",
        )

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
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

    def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
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

    # —— Operations ——

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
