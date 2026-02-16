#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
STAGE_SCRIPT="${SCRIPT_DIR}/stage-resources.sh"
ENV_FILE="${KORM_TESTING_ENV_FILE:-${REPO_ROOT}/.env.testing.local}"

OUTPUT_DIR=""
RUN_LOG_DIR=""

usage() {
  cat <<EOF
Usage: $(basename "$0") [-o path/to/destination]

Options:
  -o <path>   Persist per-suite output files in the destination directory.
  -h          Show this help message.
EOF
}

while (($# > 0)); do
  case "$1" in
    -o)
      shift
      if [ "$#" -eq 0 ]; then
        echo "Missing path for -o." >&2
        usage
        exit 1
      fi
      OUTPUT_DIR="$1"
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
  shift
done

if [ -n "${OUTPUT_DIR}" ]; then
  RUN_LOG_DIR="${OUTPUT_DIR}"
  mkdir -p "${RUN_LOG_DIR}"
fi

SUITE_KEYS=("unit" "integration" "hostile")
SUITE_CMDS=(
  "bun run test:unit"
  "bun run test:integration"
  "bun run test:hostile"
)

declare -a SUITE_STATUS
declare -a SUITE_DURATION
declare -a SUITE_LOG
declare -a SUITE_OUTPUT

OVERALL_EXIT_CODE=0
CLEANUP_STATUS=0
SUMMARY_PRINTED=0

print_summary() {
  if [ "${SUMMARY_PRINTED}" -eq 1 ]; then
    return
  fi
  SUMMARY_PRINTED=1

  echo
  echo "Full suite summary:"
  local i
  for i in "${!SUITE_KEYS[@]}"; do
    local key="${SUITE_KEYS[$i]}"
    local status="${SUITE_STATUS[$i]:-}"
    local duration="${SUITE_DURATION[$i]:-0}"
    local log_file="${SUITE_LOG[$i]:-}"
    if [ -z "${status}" ]; then
      printf '  %-12s %-8s %6s  %s\n' "${key}" "NOT RUN" "-" "${log_file:--}"
      continue
    fi
    if [ "${status}" -eq 0 ]; then
      printf '  %-12s %-8s %6ss  %s\n' "${key}" "PASS" "${duration}" "${log_file:--}"
    else
      printf '  %-12s %-8s %6ss  %s\n' "${key}" "FAIL(${status})" "${duration}" "${log_file:--}"
    fi
  done

  if [ "${CLEANUP_STATUS}" -eq 0 ]; then
    printf '  %-12s %-8s %6s  %s\n' "cleanup" "PASS" "-" "-"
  else
    printf '  %-12s %-8s %6s  %s\n' "cleanup" "FAIL" "-" "-"
  fi

  if [ "${OVERALL_EXIT_CODE}" -eq 0 ] && [ "${CLEANUP_STATUS}" -eq 0 ]; then
    echo "Overall: PASS"
  else
    echo "Overall: FAIL"
  fi

  local showed_failure=0
  for i in "${!SUITE_KEYS[@]}"; do
    local status="${SUITE_STATUS[$i]:-0}"
    if [ "${status}" -ne 0 ]; then
      if [ "${showed_failure}" -eq 0 ]; then
        echo
        echo "Failure tails (last 60 lines):"
        showed_failure=1
      fi
      local key="${SUITE_KEYS[$i]}"
      local log_file="${SUITE_LOG[$i]:-}"
      if [ -n "${log_file}" ]; then
        echo "--- ${key}: ${log_file} ---"
        tail -n 60 "${log_file}" || true
      else
        echo "--- ${key} ---"
        printf '%s\n' "${SUITE_OUTPUT[$i]}" | tail -n 60 || true
      fi
    fi
  done
}

cleanup() {
  local exit_code=$?
  trap - EXIT
  set +e

  bash "${STAGE_SCRIPT}" down >/dev/null 2>&1
  local down_status=$?
  rm -f "${ENV_FILE}"
  local rm_status=$?

  if [ "${down_status}" -ne 0 ] || [ "${rm_status}" -ne 0 ]; then
    CLEANUP_STATUS=1
  fi

  print_summary

  if [ "${OVERALL_EXIT_CODE}" -ne 0 ]; then
    exit "${OVERALL_EXIT_CODE}"
  fi
  if [ "${exit_code}" -ne 0 ]; then
    exit "${exit_code}"
  fi
  if [ "${CLEANUP_STATUS}" -ne 0 ]; then
    exit 1
  fi
  exit 0
}
trap cleanup EXIT

run_suite() {
  local i="$1"
  local key="${SUITE_KEYS[$i]}"
  local cmd="${SUITE_CMDS[$i]}"
  local log_file=""

  if [ -n "${RUN_LOG_DIR}" ]; then
    log_file="${RUN_LOG_DIR}/${key}.log"
  fi

  local start_ts
  start_ts="$(date +%s)"
  printf '[RUN ] %s\n' "${key}"

  set +e
  local output
  output="$(fish -lc "${cmd}" 2>&1)"
  local status=$?
  set -e

  local end_ts
  end_ts="$(date +%s)"
  local duration=$((end_ts - start_ts))

  SUITE_STATUS[$i]="${status}"
  SUITE_DURATION[$i]="${duration}"
  SUITE_OUTPUT[$i]="${output}"

  if [ -n "${log_file}" ]; then
    printf '%s\n' "${output}" >"${log_file}"
    SUITE_LOG[$i]="${log_file}"
  else
    SUITE_LOG[$i]=""
  fi

  if [ "${status}" -eq 0 ]; then
    printf '[PASS] %s (%ss)\n' "${key}" "${duration}"
    return
  fi

  if [ -n "${log_file}" ]; then
    printf '[FAIL] %s (%ss) -> %s\n' "${key}" "${duration}" "${log_file}"
  else
    printf '[FAIL] %s (%ss)\n' "${key}" "${duration}"
  fi

  if [ "${OVERALL_EXIT_CODE}" -eq 0 ]; then
    OVERALL_EXIT_CODE="${status}"
  fi
  return 0
}

echo "[INFO] staging local test resources"
bash "${STAGE_SCRIPT}" up

if [ ! -f "${ENV_FILE}" ]; then
  echo "Expected env file at ${ENV_FILE}, but it was not created." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

if [ -n "${RUN_LOG_DIR}" ]; then
  echo "[INFO] running suites; output files: ${RUN_LOG_DIR}"
else
  echo "[INFO] running suites; output files disabled (use -o <path> to persist)"
fi

for i in "${!SUITE_KEYS[@]}"; do
  run_suite "${i}"
done
