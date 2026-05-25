"""Host pre-checks for FastINK deployment.

Validates that required CLI tools (docker, docker compose, mountpoint)
are available, that /cvmfs is mounted, and that all CVMFS paths needed
at runtime are accessible.  Called early in every deploy to fail fast.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

REQUIRED_CVMFS_PATHS = (
    Path('/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.02/x86_64-almalinux9.6-gcc115-opt/bin/thisroot.sh'),
    Path('/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.32.06/x86_64-almalinux9.4-gcc114-opt/bin/thisroot.sh'),
    Path('/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials'),
    Path('/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.00/src/tutorials/gallery.root'),
    Path('/cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/novnc_proxy'),
    Path('/cvmfs/common.ihep.ac.cn/software/noVNC-master/utils/generateOTP.sh'),
    Path('/cvmfs/common.ihep.ac.cn/software/ipykernel/fermiPy/fermiPy_1_4_0/share/jupyter'),
    Path('/cvmfs/common.ihep.ac.cn/software/ipykernel/Julia/Julia_1_11_5/share/jupyter'),
    Path('/cvmfs/common.ihep.ac.cn/software/ipykernel/ROOT/ROOT_6_34_4/share/jupyter'),
    Path('/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/bin'),
    Path('/cvmfs/slurm.ihep.ac.cn/centos7.9/anaconda3/envs/ink/lib'),
    Path('/cvmfs/slurm.ihep.ac.cn/alma9/anaconda3/envs/jupyter/bin'),
    Path('/cvmfs/slurm.ihep.ac.cn/alma9/junokernel/share/jupyter'),
)


def require_command(name: str) -> None:
    if shutil.which(name) is None:
        raise RuntimeError(f'{name} command not found in PATH.')



def require_docker_compose() -> None:
    try:
        subprocess.run(
            ['docker', 'compose', 'version'],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError('docker compose is not available.') from exc



def require_cvmfs_mount() -> None:
    try:
        subprocess.run(
            ['mountpoint', '-q', '/cvmfs'],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError('/cvmfs is not mounted on the host.') from exc



def warm_cvmfs_path(path: Path) -> None:
    if not path.exists():
        raise RuntimeError(f'Required CVMFS path is not available on the host: {path}')
    try:
        path.stat()
    except OSError as exc:
        raise RuntimeError(f'Failed to warm CVMFS path {path}: {exc}') from exc



def warm_cvmfs_paths(paths: Iterable[Path] = REQUIRED_CVMFS_PATHS) -> None:
    for path in paths:
        warm_cvmfs_path(path)



def check_host_prerequisites(*, require_cvmfs: bool = True) -> None:
    require_command('docker')
    require_command('mountpoint')
    require_docker_compose()
    if require_cvmfs:
        require_cvmfs_mount()
        warm_cvmfs_paths()



def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Check host-side FastINK deployment prerequisites.')
    parser.add_argument('--skip-cvmfs', action='store_true', help='Skip /cvmfs mount and warmup checks.')
    args = parser.parse_args(argv)
    try:
        check_host_prerequisites(require_cvmfs=not args.skip_cvmfs)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
