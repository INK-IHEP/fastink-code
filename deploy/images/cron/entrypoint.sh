#!/bin/bash
set -uo pipefail

BASE_DIR="${FASTINK_CRON_BASE_DIR:-/opt/fastink-cron}"
CONFIG="${FASTINK_CRON_CONFIG:-$BASE_DIR/cron.ini}"
LOG_DIR="${FASTINK_CRON_LOG_DIR:-/var/log/fastink-cron}"
INK_CODE_DIR="${INK_CODE_DIR:-/ink}"
INSTALL_EDITABLE="${INSTALL_EDITABLE:-false}"
PLUGIN_PIP_PACKAGES="${PLUGIN_PIP_PACKAGES:-}"
PLUGIN_EDITABLE_DIRS="${PLUGIN_EDITABLE_DIRS:-}"
PRELOAD_SCRIPTS="${PRELOAD_SCRIPTS:-}"
PRELOAD_SCRIPT_DIRS="${PRELOAD_SCRIPT_DIRS:-}"

pids=()

current_log_file() {
    echo "$LOG_DIR/cron-$(date '+%F').log"
}

log() {
    local log_file
    log_file="$(current_log_file)"
    mkdir -p "$LOG_DIR"
    echo "$(date '+%F %T') $*" >>"$log_file"
}

run_preload_script() {
    local script="$1"
    if [[ ! -f "$script" ]]; then
        log "ERROR: preload script not found: $script"
        exit 1
    fi
    log "Running preload script: $script"
    bash "$script" >>"$(current_log_file)" 2>&1
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
                log "ERROR: preload script directory not found: $directory"
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
        log "Installing plugin package: $package"
        pip3.12 install --no-cache-dir "$package" >>"$(current_log_file)" 2>&1
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
            log "ERROR: plugin source directory not found: $directory"
            exit 1
        fi
        log "Installing editable plugin from: $directory"
        pip3.12 install -e "$directory" >>"$(current_log_file)" 2>&1
    done
}

cleanup() {
    log "Stopping container, killing all jobs..."
    for pid in "${pids[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait
    log "All jobs stopped."
    exit 0
}

trap cleanup SIGTERM SIGINT

mkdir -p "$LOG_DIR"

log "Container started"

if [[ "$INSTALL_EDITABLE" == "true" ]]; then
    log "Installing fastink in editable mode from $INK_CODE_DIR"
    if [[ -d "$INK_CODE_DIR" ]]; then
        pip3.12 install -e "$INK_CODE_DIR" >>"$(current_log_file)" 2>&1
    else
        log "ERROR: INK_CODE_DIR not found: $INK_CODE_DIR"
        exit 1
    fi
fi

install_plugin_packages "$PLUGIN_PIP_PACKAGES"
install_plugin_editable_dirs "$PLUGIN_EDITABLE_DIRS"
run_preload_scripts "$PRELOAD_SCRIPTS" "$PRELOAD_SCRIPT_DIRS"

log "Loading $CONFIG"

if [[ ! -f "$CONFIG" ]]; then
    log "ERROR: $CONFIG not found, container idle."
    while true; do sleep 3600; done
fi

start_job() {
    local name="$1"
    local script="$2"
    local interval="$3"
    local mode="$4"

    local file="$BASE_DIR/jobs/$script"
    if [[ ! -f "$file" ]]; then
        file="$BASE_DIR/$script"
    fi

    if [[ ! -f "$file" ]]; then
        log "WARN: script $script not found under $BASE_DIR/jobs or $BASE_DIR, skip job $name"
        return 0
    fi

    log "Starting job: $name ($script), interval=${interval}s, mode=$mode"

    (
        case "$mode" in
            fixed)
                while true; do
                    local run_log_file
                    run_log_file="$(current_log_file)"
                    log "[$name] fixed run start"

                    timeout "$interval" bash -c \
                        "python3.12 \"$file\" >>\"$run_log_file\" 2>&1; sleep infinity"

                    rc=$?
                    if [[ $rc -eq 124 ]]; then
                        log "[$name] killed by timeout (${interval}s)"
                    else
                        log "[$name] finished with code=$rc"
                    fi
                done
                ;;
            delay)
                while true; do
                    local run_log_file
                    run_log_file="$(current_log_file)"
                    log "[$name] delay run start"

                    python3.12 "$file" >>"$run_log_file" 2>&1
                    rc=$?

                    if [[ $rc -ne 0 ]]; then
                        log "[$name] failed with code=$rc"
                    fi

                    log "[$name] sleep $interval"
                    sleep "$interval"
                done
                ;;
            *)
                log "ERROR: unknown mode '$mode' for job $name, skip"
                ;;
        esac
    ) &

    pids+=("$!")
}

current_job=""
script=""
interval=""
mode=""

while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="${line%%;*}"
    [[ -z "$line" ]] && continue

    if [[ "$line" =~ ^\[job:(.+)\]$ ]]; then
        if [[ -n "$current_job" ]]; then
            start_job "$current_job" "$script" "$interval" "${mode:-fixed}"
        fi

        current_job="${BASH_REMATCH[1]}"
        script=""
        interval=""
        mode="fixed"
        continue
    fi

    key="${line%%=*}"
    value="${line#*=}"

    case "$key" in
        script) script="$value" ;;
        interval) interval="$value" ;;
        mode) mode="$value" ;;
    esac
done <"$CONFIG"

if [[ -n "$current_job" ]]; then
    start_job "$current_job" "$script" "$interval" "${mode:-fixed}"
fi

log "All jobs started."

wait
