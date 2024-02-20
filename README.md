# SLURM BUSTER
## Setup
- Install `polars` and [`reportseff`](https://github.com/troycomi/reportseff): `pip install polars reportseff`
- Run on a phoenix node (head node CPU doesn't have instruction set for `polars`):
```
srun python slurm_buster.py
```
## TODO
- Come up with and arguments for `slurm_buster.py` to get different stats n cover longer periods of time
- Read parquet file and actually make the leaderboard
