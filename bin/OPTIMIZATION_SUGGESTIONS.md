# CLI Commands Optimization Suggestions

## 1. 错误处理和异常管理

### 当前问题
- 缺少统一的错误处理机制
- 异常信息不够详细
- 没有日志记录

### 建议改进

在 `basecommand.py` 中添加错误处理装饰器：

```python
import functools
import logging
from typing import Callable, Any

def handle_command_errors(func: Callable) -> Callable:
    """Decorator to handle command execution errors uniformly."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            print("\nOperation cancelled by user")
            sys.exit(130)
        except Exception as e:
            logging.error(f"Command failed: {e}", exc_info=True)
            print(f"Error: {e}")
            sys.exit(1)
    return wrapper
```

在命令方法上使用：

```python
@handle_command_errors
def add_user(self):
    result = user.add_user(...)
    if result:
        print(f"{self.args.username} added!")
    else:
        print(f"{self.args.username} adding failed!")
```

## 2. 减少代码重复

### 当前问题
`user.py` 中多个 namespace 函数重复定义相同的参数。

### 建议改进

创建参数组复用机制：

```python
class User(TopLevelCommand):

    @staticmethod
    def _add_user_identity_args(parser: argparse.ArgumentParser, required: bool = False) -> None:
        """Add common user identity arguments."""
        parser.add_argument(
            "--username",
            required=required,
            help="Username of the user",
        )
        parser.add_argument(
            "--email",
            required=required,
            help="Email of the user",
        )
        parser.add_argument(
            "--uid",
            required=required,
            type=int,
            help="UID of the user",
        )

    def add_namespace(self, parser: argparse.ArgumentParser) -> None:
        self._add_user_identity_args(parser, required=True)

    def list_namespace(self, parser: argparse.ArgumentParser) -> None:
        self._add_user_identity_args(parser, required=False)

    def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
        self._add_user_identity_args(parser, required=True)
```

## 3. 参数验证

### 当前问题
- 很多操作的参数都是 `required=False`，但实际上是必需的
- 缺少参数组合的验证（如至少需要 username 或 email）

### 建议改进

添加参数验证方法：

```python
class User(TopLevelCommand):

    def _validate_user_identity(self) -> None:
        """Validate that at least one user identifier is provided."""
        if not any([self.args.username, self.args.email, self.args.uid]):
            raise ValueError("At least one of --username, --email, or --uid must be provided")

    def delete_user(self):
        self._validate_user_identity()
        result = user.delete_user(
            username=self.args.username,
            email=self.args.email,
            uid=self.args.uid,
        )
        if result:
            print(f"User deleted successfully!")
        else:
            print(f"Failed to delete user!")
```

或者使用 argparse 的互斥组：

```python
def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--username", help="Username of the user")
    group.add_argument("--email", help="Email of the user")
    group.add_argument("--uid", type=int, help="UID of the user")
```

## 4. 改进输出格式

### 当前问题
- JSON 输出不够用户友好
- 缺少表格格式输出
- 成功/失败消息不一致

### 建议改进

添加表格输出支持（使用 `tabulate` 库）：

```python
from tabulate import tabulate

class User(TopLevelCommand):

    def list_users(self) -> None:
        result = user.list_users()

        # Filter results
        if self.args.username:
            result = [u for u in result if u["username"] == self.args.username]
        if self.args.email:
            result = [u for u in result if u["email"] == self.args.email]

        if not result:
            print("No users found")
            return

        # Format as table
        headers = ["Username", "Email", "UID", "Created"]
        rows = [[u.get("username"), u.get("email"), u.get("uid"), u.get("created_at")]
                for u in result]

        print(tabulate(rows, headers=headers, tablefmt="grid"))
        print(f"\nTotal: {len(result)} user(s)")
```

添加 `--format` 参数支持多种输出格式：

```python
def list_namespace(self, parser: argparse.ArgumentParser) -> None:
    self._add_user_identity_args(parser, required=False)
    parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="Output format",
    )
```

## 5. 添加确认提示

### 当前问题
删除操作没有确认提示，容易误操作。

### 建议改进

```python
def delete_user(self):
    self._validate_user_identity()

    # Show confirmation prompt
    user_info = f"username={self.args.username}" if self.args.username else \
                f"email={self.args.email}" if self.args.email else \
                f"uid={self.args.uid}"

    confirm = input(f"Are you sure you want to delete user ({user_info})? [y/N]: ")
    if confirm.lower() != 'y':
        print("Operation cancelled")
        return

    result = user.delete_user(
        username=self.args.username,
        email=self.args.email,
        uid=self.args.uid,
    )
    if result:
        print(f"User deleted successfully!")
    else:
        print(f"Failed to delete user!")
```

添加 `--yes` 或 `-y` 参数跳过确认：

```python
def delete_namespace(self, parser: argparse.ArgumentParser) -> None:
    self._add_user_identity_args(parser, required=True)
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt",
    )
```

## 6. 完善类型注解

### 当前问题
类型注解不完整，影响代码可维护性。

### 建议改进

```python
from typing import Optional, Dict, List, Any

class User(TopLevelCommand):

    def list_users(self) -> None:
        result: List[Dict[str, Any]] = user.list_users()
        # ...

    def add_user(self) -> None:
        result: bool = user.add_user(
            username=self.args.username,
            email=self.args.email,
            uid=self.args.uid,
        )
        # ...
```

## 7. 修复拼写错误

### 需要修复的地方

**user.py:14**
```python
# 修改前
return "Users managerment"

# 修改后
return "Users management"
```

**user.py:213**
```python
# 修改前
return "Permissions managerment"

# 修改后
return "Permissions management"
```

## 8. 显示命令执行时间

### 当前问题
`Commands.__call__` 计算了执行时间但没有使用。

### 建议改进

```python
class Commands:
    def __call__(self) -> None:
        start_time = time.time()
        self._run_command()
        end_time = time.time()

        # Add verbose flag to show execution time
        if hasattr(self.args, 'verbose') and self.args.verbose:
            elapsed = end_time - start_time
            print(f"\nExecution time: {elapsed:.2f}s")
```

在主 parser 中添加 verbose 选项：

```python
@staticmethod
def _add_parsers() -> argparse.ArgumentParser:
    # ...
    options_group.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show verbose output including execution time"
    )
    # ...
```

## 9. 添加配置文件支持

### 建议改进

允许通过配置文件设置默认值：

```python
# ~/.inkrc or /etc/ink/config.ini
[defaults]
format = table
verbose = false

[user]
default_permissions = CentOS7,AlmaLinux9
```

在 Commands 类中加载配置：

```python
import configparser
from pathlib import Path

class Commands:
    @staticmethod
    def _load_config() -> configparser.ConfigParser:
        config = configparser.ConfigParser()
        config_paths = [
            Path.home() / ".inkrc",
            Path("/etc/ink/config.ini"),
        ]
        for path in config_paths:
            if path.exists():
                config.read(path)
                break
        return config
```

## 10. 添加批量操作支持

### 建议改进

支持从文件批量导入用户：

```python
def _operations(self) -> dict[str, OperationDict]:
    return {
        # ... existing operations ...
        "import": {
            "call": self.import_users,
            "docs": "Import users from CSV/JSON file",
            "namespace": self.import_namespace,
        },
    }

def import_namespace(self, parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--file",
        required=True,
        help="Path to CSV or JSON file containing user data",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Input file format",
    )

def import_users(self) -> None:
    import csv
    import json

    with open(self.args.file) as f:
        if self.args.format == "csv":
            reader = csv.DictReader(f)
            users = list(reader)
        else:
            users = json.load(f)

    success_count = 0
    fail_count = 0

    for user_data in users:
        try:
            result = user.add_user(**user_data)
            if result:
                success_count += 1
            else:
                fail_count += 1
        except Exception as e:
            print(f"Failed to import user {user_data.get('username')}: {e}")
            fail_count += 1

    print(f"\nImport complete: {success_count} succeeded, {fail_count} failed")
```

## 11. 添加 Shell 补全支持

### 建议改进

生成 bash/zsh 补全脚本：

```python
def _operations(self) -> dict[str, OperationDict]:
    return {
        # ... existing operations ...
        "completion": {
            "call": self.generate_completion,
            "docs": "Generate shell completion script",
            "namespace": self.completion_namespace,
        },
    }

def completion_namespace(self, parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--shell",
        choices=["bash", "zsh", "fish"],
        required=True,
        help="Shell type",
    )

def generate_completion(self) -> None:
    # Generate completion script based on shell type
    if self.args.shell == "bash":
        print(self._generate_bash_completion())
    elif self.args.shell == "zsh":
        print(self._generate_zsh_completion())
```

## 12. 改进 bin/ink 入口脚本

### 当前问题
- 缺少错误处理
- 没有版本信息

### 建议改进

```python
#!/usr/bin/env python3
import sys
from fastink.commands.basecommand import Commands

__version__ = "1.0.0"

def main():
    try:
        parser_object = Commands._add_parsers()

        # Add version argument
        parser_object.add_argument(
            "--version",
            action="version",
            version=f"ink {__version__}",
        )

        args = parser_object.parse_args()
        Commands(args=args)()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
```

## 优先级建议

### 高优先级（建议立即实施）
1. 修复拼写错误（managerment → management）
2. 添加参数验证
3. 改进错误处理
4. 添加删除操作的确认提示

### 中优先级（建议近期实施）
5. 减少代码重复
6. 完善类型注解
7. 改进输出格式（表格支持）
8. 显示执行时间

### 低优先级（可选功能）
9. 配置文件支持
10. 批量操作
11. Shell 补全
12. 更多输出格式（CSV, YAML 等）

## 测试建议

建议为命令添加单元测试：

```python
# tests/test_commands.py
import pytest
from unittest.mock import Mock, patch
from fastink.commands.user import User

def test_add_user_success():
    args = Mock(username="test", email="test@test.com", uid=12345)
    with patch('fastink.auth.user.add_user', return_value=True):
        user_cmd = User(args)
        user_cmd.add_user()  # Should print success message

def test_add_user_validation_error():
    args = Mock(username=None, email=None, uid=None)
    user_cmd = User(args)
    with pytest.raises(ValueError):
        user_cmd._validate_user_identity()
```
