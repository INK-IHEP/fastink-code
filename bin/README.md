# INK Command Line Tools

This directory contains command-line tools for managing INK (Interactive aNalysis worKbench) system.

## Installation

The `ink` command is automatically installed when you install the fastink package:

```bash
cd /path/to/ink-code
pip install -e .
```

Or using the project's virtual environment:

```bash
/path/to/.venv/bin/pip install -e .
```

## Configuration

Before using the CLI tools, ensure you have a valid configuration file at `src/fastink/misc/config.yml` or set the `INK_CONFIG_FILE` environment variable to point to your config file.

## Available Commands

### General Help

```bash
ink --help                    # Show all available commands
ink <command> --help          # Show help for a specific command
```

### User Management

Manage users in the INK system:

```bash
# List all users
ink user list

# List specific user
ink user list --username=testuser

# Add a new user
ink user add --username=testuser --email=test@example.com --uid=12345

# Update user information
ink user update --username=testuser --new-username=newuser --new-email=new@example.com --new-uid=54321

# Delete a user
ink user delete --username=testuser --email=test@example.com --uid=12345

# Get user token
ink user token --username=testuser
```

### User Permission Management

Manage user-specific permissions:

```bash
# List user's permissions
ink user permission list --username=testuser

# Add permission to user
ink user permission add --username=testuser --permission=CentOS7

# Delete permission from user
ink user permission delete --username=testuser --permission=CentOS7
```

### Permission Management

Manage system-wide permissions:

```bash
# List all permissions
ink permission list

# Add a new permission
ink permission add --permission=AlmaLinux9

# Delete a permission (also removes it from all users)
ink permission delete --permission=AlmaLinux9
```

**Note:** Deleting a permission will automatically remove it from all users who have that permission.

## Command Structure

The INK CLI follows a hierarchical command structure:

```
ink
├── user                      # User management
│   ├── add                   # Add user
│   ├── list                  # List users
│   ├── delete                # Delete user
│   ├── update                # Update user
│   ├── token                 # Get user token
│   └── permission            # User permission management
│       ├── list              # List user permissions
│       ├── add               # Add permission to user
│       └── delete            # Remove permission from user
└── permission                # System permission management
    ├── list                  # List all permissions
    ├── add                   # Add new permission
    └── delete                # Delete permission
```

## Adding New Commands

To add a new top-level command:

1. Create a new file in `src/fastink/commands/` (e.g., `mycommand.py`)
2. Create a class that inherits from `TopLevelCommand`
3. Implement required methods:
   - `module_help()` - Command description
   - `usage_example()` - Usage examples
   - `_operations()` - Available operations (add, list, delete, etc.)
   - `implemented_subcommands()` - Nested subcommands (if any)
4. Import the class in `src/fastink/commands/__init__.py`

Example:

```python
from fastink.commands.basecommand import TopLevelCommand, OperationDict

class MyCommand(TopLevelCommand):
    def module_help(self) -> str:
        return "My command description"

    def usage_example(self) -> list[str]:
        return ["ink mycommand list", "ink mycommand add --name=test"]

    def _operations(self) -> dict[str, OperationDict]:
        return {
            "list": {
                "call": self.list_items,
                "docs": "List all items",
            },
        }

    def implemented_subcommands(self) -> dict[str, type["CommandBase"]]:
        return {}

    def list_items(self):
        print("Listing items...")
```

Then import it in `__init__.py`:

```python
from fastink.commands.mycommand import MyCommand
```

The command will be automatically discovered and available as `ink mycommand`.

## Troubleshooting

### ModuleNotFoundError: No module named 'fastink'

Make sure you've installed the package in editable mode:

```bash
pip install -e /path/to/ink-code
```

### FileNotFoundError: config.yml

Set the `INK_CONFIG_FILE` environment variable or ensure the config file exists at `src/fastink/misc/config.yml`:

```bash
export INK_CONFIG_FILE=/path/to/your/config.yml
ink --help
```

### Permission denied

Make sure the `ink` script is executable:

```bash
chmod +x bin/ink
```

## Development

When developing new commands, you can test them immediately after installation in editable mode. Changes to command files will be reflected without reinstalling.

For more information about the INK system architecture, see the main project documentation.
