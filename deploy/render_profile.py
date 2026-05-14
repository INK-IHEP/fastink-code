#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any

from lib.defaults import normalize_answers, parse_override_value
from lib.paths import build_runtime_paths
from lib.render import render_bundle


def load_answers(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render FastINK deploy templates without interaction.")
    parser.add_argument("--profile", choices=["quickstart", "custom"], required=True)
    parser.add_argument("--answers-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--db-data-dir", type=Path)
    parser.add_argument("--redis-data-dir", type=Path)
    parser.add_argument("--etc-init-dir", type=Path)
    parser.add_argument("--tmp-dir", type=Path)
    parser.add_argument("--plugins-dir", type=Path)
    parser.add_argument("--keys-dir", type=Path)
    parser.add_argument("--preload-server-dir", type=Path)
    parser.add_argument("--preload-cron-dir", type=Path)
    parser.add_argument("--preload-rootbrowse-dir", type=Path)
    parser.add_argument("--rootbrowse-authorized-keys-path", type=Path)
    parser.add_argument("--server-ssh-private-key-path", type=Path)
    parser.add_argument("--config-overlay", type=Path, action="append", default=[])
    parser.add_argument("--compose-overlay", type=Path, action="append", default=[])
    parser.add_argument("--set", dest="overrides", action="append", default=[])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    answers = load_answers(args.answers_file.resolve())
    for override in args.overrides:
        if "=" not in override:
            raise ValueError(f"Invalid --set override: {override}")
        key, value = override.split("=", 1)
        answers[key] = parse_override_value(key, value)

    answers = normalize_answers(answers, profile=args.profile, deploy_dir=args.output_dir.resolve())
    output_dir, paths = build_runtime_paths(
        output_dir=args.output_dir.resolve(),
        data_root=Path(args.data_root or (args.output_dir.resolve() / "data")),
        enable_nginx=bool(answers.get("enable_nginx")),
        enable_xrootd=bool(answers.get("enable_xrootd")),
        db_data_dir=args.db_data_dir.resolve() if args.db_data_dir else None,
        redis_data_dir=args.redis_data_dir.resolve() if args.redis_data_dir else None,
        etc_init_dir=args.etc_init_dir.resolve() if args.etc_init_dir else None,
        tmp_dir=args.tmp_dir.resolve() if args.tmp_dir else None,
        plugins_dir=args.plugins_dir.resolve() if args.plugins_dir else None,
        keys_dir=args.keys_dir.resolve() if args.keys_dir else None,
        preload_server_dir=args.preload_server_dir.resolve() if args.preload_server_dir else None,
        preload_cron_dir=args.preload_cron_dir.resolve() if args.preload_cron_dir else None,
        preload_rootbrowse_dir=args.preload_rootbrowse_dir.resolve() if args.preload_rootbrowse_dir else None,
        rootbrowse_authorized_keys_path=args.rootbrowse_authorized_keys_path.resolve() if args.rootbrowse_authorized_keys_path else None,
        server_ssh_private_key_path=args.server_ssh_private_key_path.resolve() if args.server_ssh_private_key_path else None,
    )
    bundle = render_bundle(
        args.profile,
        answers,
        paths,
        output_dir,
        config_overlay_paths=[path.resolve() for path in args.config_overlay],
        compose_overlay_paths=[path.resolve() for path in args.compose_overlay],
    )

    for relative_path, content in bundle.items():
        write_file(output_dir / relative_path, content)
    write_file(output_dir / "answers.json", json.dumps(answers, indent=2, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
