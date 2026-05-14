import asyncio

from fastink.computing.crond.update_job_status import resert_start_end_time


async def main():
    await resert_start_end_time()


if __name__ == "__main__":
    asyncio.run(main())
