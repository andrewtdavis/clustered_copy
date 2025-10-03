#!/usr/bin/env bash
#SBATCH -J rsync_tree
#SBATCH -p batch
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=24:00:00
#SBATCH -o logs/%x_%A_%a.out
#SBATCH -e logs/%x_%A_%a.err
set -euo pipefail

# Set these via environment or edit here:
SRC="${SRC:?set SRC to the source root}"
DST="${DST:?set DST to the destination root}"

SHARD=$(printf "shards/shard_%05d.nul" "${SLURM_ARRAY_TASK_ID:?no array id}")
test -f "$SHARD" || { echo "Missing shard: $SHARD" >&2; exit 2; }

# Nice/ionice are optional if this hits a shared NAS
ionice -c2 -n4 2>/dev/null || true
renice +5 -p $$ 2>/dev/null || true
ulimit -n 1048576 || true

exec rsync \
  -rt --no-perms --no-owner --no-group \
  --files-from="$SHARD" --from0 \
  --info=progress2 --info=stats2 \
  "$SRC"/ "$DST"/