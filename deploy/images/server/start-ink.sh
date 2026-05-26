#!/bin/bash
set -euo pipefail

cd /ink

CONFIG_FILE="${INK_CONFIG_FILE:-/ink/config.yml}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"
WORKERS="${WORKERS:-4}"
PLUGIN_PIP_PACKAGES="${PLUGIN_PIP_PACKAGES:-}"
PLUGIN_EDITABLE_DIRS="${PLUGIN_EDITABLE_DIRS:-}"
PRELOAD_SCRIPTS="${PRELOAD_SCRIPTS:-}"
PRELOAD_SCRIPT_DIRS="${PRELOAD_SCRIPT_DIRS:-}"

run_preload_script() {
    local script="$1"
    if [[ ! -f "$script" ]]; then
        echo "Preload script not found: $script" >&2
        exit 1
    fi
    echo "Running preload script: $script"
    bash "$script"
}

run_preload_scripts() {
    local scripts="$1"
    local directories="$2"
    local directory
    local script

    if [[ -n "$directories" ]]; then
        IFS=',' read -ra dir_list <<< "$directories"
        for directory in "${dir_list[@]}"; do
            directory="${directory#"${directory%%[![:space:]]*}"}"
            directory="${directory%"${directory##*[![:space:]]}"}"
            if [[ -z "$directory" ]]; then
                continue
            fi
            if [[ ! -d "$directory" ]]; then
                echo "Preload script directory not found: $directory" >&2
                exit 1
            fi
            while IFS= read -r script; do
                run_preload_script "$script"
            done < <(find "$directory" -maxdepth 1 -type f ! -name '.*' | sort)
        done
    fi

    if [[ -n "$scripts" ]]; then
        IFS=',' read -ra script_list <<< "$scripts"
        for script in "${script_list[@]}"; do
            script="${script#"${script%%[![:space:]]*}"}"
            script="${script%"${script##*[![:space:]]}"}"
            if [[ -z "$script" ]]; then
                continue
            fi
            run_preload_script "$script"
        done
    fi
}

install_plugin_packages() {
    local packages="$1"
    if [[ -z "$packages" ]]; then
        return
    fi

    IFS=',' read -ra package_list <<< "$packages"
    for package in "${package_list[@]}"; do
        package="${package#"${package%%[![:space:]]*}"}"
        package="${package%"${package##*[![:space:]]}"}"
        if [[ -z "$package" ]]; then
            continue
        fi
        echo "Installing plugin package: $package"
        pip3.12 install --no-cache-dir "$package"
    done
}

install_plugin_editable_dirs() {
    local directories="$1"
    if [[ -z "$directories" ]]; then
        return
    fi

    IFS=',' read -ra dir_list <<< "$directories"
    for directory in "${dir_list[@]}"; do
        directory="${directory#"${directory%%[![:space:]]*}"}"
        directory="${directory%"${directory##*[![:space:]]}"}"
        if [[ -z "$directory" ]]; then
            continue
        fi
        if [[ ! -d "$directory" ]]; then
            echo "Plugin source directory not found: $directory" >&2
            exit 1
        fi
        echo "Installing editable plugin from: $directory"
        pip3.12 install -e "$directory"
    done
}

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "Config file not found: $CONFIG_FILE" >&2
    echo "Mount it from docker-compose or set INK_CONFIG_FILE to a valid path." >&2
    exit 1
fi

mkdir -p /tmp/ink
chmod 1777 /tmp/ink

run_preload_scripts "$PRELOAD_SCRIPTS" "$PRELOAD_SCRIPT_DIRS"

if [[ "${INSTALL_EDITABLE:-false}" == "true" ]]; then
    pip3.12 install -e /ink
fi

install_plugin_packages "$PLUGIN_PIP_PACKAGES"
install_plugin_editable_dirs "$PLUGIN_EDITABLE_DIRS"

if [[ "${INIT_DATABASE_ON_START:-true}" == "true" ]]; then
    python3.12 /ink/tools/init_database.py
fi

if [[ "${INK_PRODUCTION:-false}" == "true" ]]; then
    exec python3.12 -m uvicorn fastink.main:app --workers "$WORKERS" --host "$HOST" --port "$PORT"
fi

exec python3.12 -m uvicorn fastink.main:app --reload --host "$HOST" --port "$PORT"
