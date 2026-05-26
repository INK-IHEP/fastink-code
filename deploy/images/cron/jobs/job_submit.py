import asyncio

from fastink.computing.crond.update_job_status import submit_job_from_redis


async def main():
    await submit_job_from_redis()


if __name__ == "__main__":
    asyncio.run(main())
