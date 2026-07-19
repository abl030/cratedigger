#!/usr/bin/env bash
# Randomized real-PostgreSQL/real-Beets lifecycle hammer for issue #743.
#
# Run inside nix-shell. This intentionally remains separate from
# scripts/run_tests.sh: its default 25 x 100 stateful budget is operator work,
# not a standard-suite gate.
set -euo pipefail

cd "$(dirname "$0")/.."

usage() {
    cat <<'EOF'
Usage: scripts/world_model_burst.sh [options]

Run the randomized real-storage lifecycle hammer against a fresh ephemeral
PostgreSQL server and disposable Beets library.

Options:
  --examples N      generated worlds (default: 25)
  --steps N         stateful steps per world (default: 100)
  --database PATH   Hypothesis replay database (default: .hypothesis/world-model)
  --print-config    print the resolved configuration without starting a world
  -h, --help        show this help

Environment equivalents: CRATEDIGGER_WORLD_EXAMPLES,
CRATEDIGGER_WORLD_STEPS, and CRATEDIGGER_WORLD_DATABASE.
EOF
}

examples="${CRATEDIGGER_WORLD_EXAMPLES:-25}"
steps="${CRATEDIGGER_WORLD_STEPS:-100}"
database="${CRATEDIGGER_WORLD_DATABASE:-.hypothesis/world-model}"
print_config=false

while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --examples)
            [[ "$#" -ge 2 ]] || { echo "--examples requires a value" >&2; exit 2; }
            examples="$2"
            shift 2
            ;;
        --steps)
            [[ "$#" -ge 2 ]] || { echo "--steps requires a value" >&2; exit 2; }
            steps="$2"
            shift 2
            ;;
        --database)
            [[ "$#" -ge 2 ]] || { echo "--database requires a value" >&2; exit 2; }
            database="$2"
            shift 2
            ;;
        --print-config)
            print_config=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

if ! positive_integer "$examples"; then
    echo "examples must be a positive integer (got '$examples')" >&2
    exit 2
fi
if ! positive_integer "$steps"; then
    echo "steps must be a positive integer (got '$steps')" >&2
    exit 2
fi
if [[ -z "$database" ]]; then
    echo "database path must be non-empty" >&2
    exit 2
fi

# tests/conftest.py accepts an externally supplied TEST_DB_DSN for ordinary
# integration-test use. A hammer must never inherit that authority: force the
# fixture to start its own disposable PostgreSQL instance every time.
unset TEST_DB_DSN
if [[ -v TEST_DB_DSN ]]; then
    postgres_mode=external
else
    postgres_mode=ephemeral
fi

export CRATEDIGGER_WORLD_EXAMPLES="$examples"
export CRATEDIGGER_WORLD_STEPS="$steps"
export CRATEDIGGER_WORLD_RANDOMIZED=1
export CRATEDIGGER_WORLD_DATABASE="$database"

if [[ "$print_config" == true ]]; then
    echo "examples=$examples"
    echo "steps=$steps"
    echo "randomized=true"
    echo "postgres=$postgres_mode"
    echo "beets=disposable"
    echo "engine=in-process-production-adapter"
    echo "database=$database"
    exit 0
fi

echo "world-model burst: examples=$examples steps=$steps database=$database"
echo "storage: fresh ephemeral PostgreSQL + disposable real Beets library"
started=$SECONDS
set +e
python3 -m unittest tests.world_model.state_machine -v
status=$?
set -e
elapsed=$((SECONDS - started))

if [[ "$status" -ne 0 ]]; then
    echo "world-model burst: FAILED after ${elapsed}s" >&2
    echo "Promote the shrunk operation sequence to TestPinnedLifecycleWorld;" >&2
    echo "never commit the replay database or an opaque failure artifact." >&2
    exit "$status"
fi

echo "world-model burst: ALL GREEN in ${elapsed}s"
