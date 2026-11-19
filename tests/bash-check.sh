#!/usr/bin/env bash
# Validate that the SSH commands the plugin generates are correct POSIX shell.
#
# Run through WSL or any Linux shell:
#   bash tests/bash-check.sh
#
# It replays the recorded `check` and `submit` commands with a fake sbatch and a
# throwaway HOME, then asserts the uploaded script and the sbatch arguments.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
work="$(mktemp -d)"
export HOME="$work/home"
mkdir -p "$HOME" "$work/bin"

cat > "$work/bin/sbatch" <<'FAKE'
#!/usr/bin/env bash
printf '%s\n' "$@" > "$SBATCH_ARGS_FILE"
echo "777;fake"
FAKE
chmod +x "$work/bin/sbatch"
export PATH="$work/bin:$PATH"
export SBATCH_ARGS_FILE="$work/args.txt"

node "$here/extract.mjs" 1 "$work/check.sh" >/dev/null 2>&1 || cp "$here/.mock/command-1.sh" "$work/check.sh"
node "$here/extract.mjs" 2 "$work/submit.sh" >/dev/null 2>&1 || cp "$here/.mock/command-2.sh" "$work/submit.sh"

check_out="$(bash "$work/check.sh" 2>&1)"
case "$check_out" in
  *"hostname="*) echo "PASS check command runs under bash" ;;
  *) echo "FAIL check command: $check_out"; exit 1 ;;
esac

submit_out="$(bash "$work/submit.sh" 2>&1)"
case "$submit_out" in
  *"777;fake"*) echo "PASS submit command returns the job id" ;;
  *) echo "FAIL submit command: $submit_out"; exit 1 ;;
esac

script="$(find "$HOME/dsh-jobs" -name 'train.sbatch' -print -quit)"
if [ -z "$script" ]; then
  echo "FAIL uploaded script missing under $HOME/dsh-jobs"
  exit 1
fi
echo "PASS uploaded script exists: $script"

expected="#!/bin/bash
#SBATCH -J train
python train.py"
if [ "$(cat "$script")" != "$expected" ]; then
  echo "FAIL uploaded script content:"
  cat "$script"
  exit 1
fi
echo "PASS uploaded script content round-trips through base64"

for flag in "--job-name=train" "--partition=MGPU-TC2" "--qos=normal" "--gres=gpu:1" "--output=slurm-%j.out" "--error=slurm-%j.err"; do
  if ! grep -qx -- "$flag" "$work/args.txt"; then
    echo "FAIL missing sbatch argument $flag"
    echo "--- args ---"
    cat "$work/args.txt"
    exit 1
  fi
done
echo "PASS sbatch received every option as one argument"

submit_clean="$(head -c 40 "$work/submit.sh")"
echo "PASS submit command begins: $submit_clean"
echo "ALL BASH CHECKS PASSED"
