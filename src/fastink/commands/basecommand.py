import argparse
import sys
import time
from abc import ABC, abstractmethod
from typing import Dict, Optional, Type
from typing_extensions import NotRequired, TypedDict


class OperationDict(TypedDict):
    """Defines the structure for command operations."""

    call: callable
    docs: NotRequired[str]
    namespace: NotRequired[callable]


class CommandBase(ABC):
    """Abstract base class for building hierarchical CLI commands."""

    def __init__(self, args: argparse.Namespace) -> None:
        """
        Initialize base command properties.

        Args:
            args: Parsed command-line arguments
        """
        self.COMMAND_NAME = sys.argv[0].split("/")[-1]  # Extract executable name
        self.PARSER_NAME = self.__class__.__name__.lower()  # Command name from class
        self.args = args

    # Abstract methods that must be implemented by subclasses
    @abstractmethod
    def module_help(self) -> str:
        """Return description for this command group."""
        raise NotImplementedError

    @abstractmethod
    def usage_example(self) -> list[str]:
        """Return list of usage examples."""
        raise NotImplementedError

    @abstractmethod
    def _operations(self) -> Dict[str, OperationDict]:
        """Define operations available for this command."""
        raise NotImplementedError

    @abstractmethod
    def implemented_subcommands(self) -> Dict[str, Type["CommandBase"]]:
        """Define subcommands under this command."""
        raise NotImplementedError

    def _help(self, level: int = 0) -> str:
        """
        Generate hierarchical help text with indentation.

        Args:
            level: Current depth in command hierarchy (for indentation)

        Returns:
            Formatted help string with command tree
        """
        indent = "  " * level
        help_str = f"{indent}{self.module_help()}\n"

        # Add operations for current level
        if operations := self._operations():
            help_str += f"\n{indent}Operations:\n"
            for name, op in operations.items():
                help_str += f"{indent}  {name}: {op.get('docs', 'No description')}\n"

        # Recursively add subcommands
        if subcommands := self.implemented_subcommands():
            help_str += f"\n{indent}Subcommands:\n"
            for name, cmd_cls in subcommands.items():
                subcmd = cmd_cls(self.args)
                help_str += (
                    f"{indent}  {name}: {subcmd.module_help()}\n"
                    f"{subcmd._help(level + 2)}"  # Recursive call
                )

        # Add usage examples at base level
        if level == 0:
            examples = "\n".join(self.usage_example())
            help_str += f"\n{indent}Examples:\n{indent}{examples}"

        return help_str

    def _add_arguments(
        self, parser: argparse.ArgumentParser, operation: OperationDict
    ) -> None:
        """Helper to add arguments for an operation."""
        if namespace_func := operation.get("namespace"):
            namespace_func(parser)

    def parser(self, subparser: argparse._SubParsersAction) -> argparse.ArgumentParser:
        """Build command parser with hierarchical structure."""
        # Configure main parser for this command
        command_parser = subparser.add_parser(
            self.PARSER_NAME,
            description=self._help(),
            formatter_class=argparse.RawDescriptionHelpFormatter,
            help=self.module_help(),
        )

        # Create subparsers container for nested commands
        subparsers = command_parser.add_subparsers(
            dest=f"{self.PARSER_NAME}_subcommand",
            title=f"Subcommands under {self.PARSER_NAME}",
            metavar="SUBCOMMAND",
        )

        # Register subcommands recursively
        for name, cmd_cls in self.implemented_subcommands().items():
            subcmd = cmd_cls(self.args)
            subcmd.parser(subparsers)

        # Register direct operations
        for name, operation in self._operations().items():
            op_parser = subparsers.add_parser(
                name, help=operation.get("docs", "No description available")
            )
            self._add_arguments(op_parser, operation)

        return command_parser

    def _execute(self, subcommand: Optional[str], verb: Optional[str]) -> None:
        """Execute commands based on parsed arguments."""
        # Check direct operations first
        if subcommand in self._operations():
            self._operations()[subcommand]["call"]()
            return

        # Handle subcommand recursion
        if subcommand and (
            subcmd_cls := self.implemented_subcommands().get(subcommand)
        ):
            subcmd = subcmd_cls(self.args)
            next_subcommand = getattr(self.args, f"{subcommand}_subcommand", None)
            subcmd(next_subcommand)  # type: ignore

    def __call__(self, subcommand: Optional[str] = None) -> None:
        """Entry point for command execution."""
        # Resolve command hierarchy from arguments
        target_subcommand = subcommand or getattr(
            self.args, f"{self.PARSER_NAME}_subcommand", None
        )
        next_verb = getattr(self.args, "verb", None)

        # Show help if no subcommand specified
        if not target_subcommand:
            print(self._help())
            return

        self._execute(target_subcommand, next_verb)


class Commands:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args

    @staticmethod
    def _all_commands() -> dict[str, type[CommandBase]]:
        command_map = {
            child.__name__.lower(): child for child in CommandBase.__subclasses__()
        }
        return command_map

    @staticmethod
    def _add_parsers() -> argparse.ArgumentParser:
        all_commands = Commands._all_commands()
        groups = ""
        for command_name, command in all_commands.items():
            help = command(None).module_help().split("\n")[0]
            groups += f"    {command_name.ljust(20)}{help}\n"
        description = "INK CLI"
        parser = argparse.ArgumentParser(
            description=description,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )

        subparsers = parser.add_subparsers(dest="command", help=argparse.SUPPRESS)
        for command in all_commands.values():
            command(None).parser(subparsers)

        return parser

    def _run_command(self) -> Optional[int]:
        try:
            command_class = Commands._all_commands()[self.args.command]
        except KeyError as e:
            sys.exit(1)

        return command_class(self.args)()

    def __call__(self) -> None:
        start_time = time.time()
        self._run_command()
        end_time = time.time()
