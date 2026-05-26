import asyncio

from fastink.computing.crond.update_job_status import update_completed_jobs


async def main():
    await update_completed_jobs()


if __name__ == "__main__":
    asyncio.run(main())
