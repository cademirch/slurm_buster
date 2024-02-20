import asyncio
import subprocess
import polars as pl
import time

with open("users.txt", "r") as f:
    users = [line.strip() for line in f.readlines()]
CONCURRENT_LIMIT = 40


async def run_command(user, semaphore):
    async with semaphore:

        cmd = [
            "reportseff",
            "--since",
            "d=7",
            "-u",
            user,
            "-p",
            "-s",
            "COMPLETED,FAILED,CANCELLED",
        ]
        start = time.time()
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            df = pl.read_csv(
                stdout,
                separator="|",
                null_values="---",
                dtypes={
                    "JobID": pl.String,
                    "Elapsed": pl.String,
                    "State": pl.String,
                    "TimeEff": pl.Float32,
                    "CPUEff": pl.Float32,
                    "MemEff": pl.Float32,
                },
            )
            df = df.with_columns(pl.lit(user).alias("user"))
            print(f"{user}, {time.time() - start:.4f}s", flush=True)
            return df
        else:
            print(stderr.decode())


async def main():
    semaphore = asyncio.Semaphore(CONCURRENT_LIMIT)

    tasks = [run_command(user, semaphore) for user in users]
    results = await asyncio.gather(*tasks)
    results = [df for df in results if df is not None]
    df = pl.concat(results)
    df.write_parquet("test.parquet")


if __name__ == "__main__":
    asyncio.run(main())
