# Clustered Copy

_This will shard all the files in a specified path, and then those lists can be used to parallelize the copy across a Slurm cluster to increase throughput._

## File Structure
- `two_phase_dir_first.py` - This will walk the file tree, and create the shards
- `rsync_shard.sbatch` - this is the slurm job to start copies based on the created shards as input files.



## Process

1. Create a work directory in a path accessible to all cluster nodes. This will store the shards and logs
2. Create the shards and logs directories
    ```
    mkdir -p logs shards
    ```
3. Create the source and destination paths
    ```
    export SRC="/path/to/source"
    export DST="/path/to/dest"
    ```
4. Walk the file tree and create the shards (this is recommended to run as close to the source storage as possible, run from the working directory). Edit shards, workers1 (dir walk), workers2 (file walk) and excludes to match your needs.
    ```
   python3 two_phase_dir_first.py \
   --root "$SRC" \
   --outdir shards \
   --shards 200 \
   --mode bydir \
   --workers1 48 \
   --workers2 64 \
   --exclude .snapshot
   ```
5. Launch the jobs matching shards. Make sure to match shards to sequence.
    ```
    for i in $(seq 0 200); do   sbatch --array=${i}-${i}%1 rsync_shard.sbatch   sleep 0.05; done
    ```