import asyncio
import json
import os

import asyncpg
from redis.asyncio import Redis

DATABASE_URL = os.environ["DATABASE_URL"]
REDIS_URL = os.environ["REDIS_URL"]
QUEUE = "student_registrations"
EVENTS = "student_events"


async def run() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL)
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        while True:
            _, payload = await redis.blpop(QUEUE)
            student = json.loads(payload)
            try:
                await pool.execute(
                    "INSERT INTO students (name, email, course, phone) VALUES ($1, $2, $3, $4) ON CONFLICT (email) DO NOTHING",
                    student["name"], student["email"], student["course"], student["phone"],
                )
                await redis.publish(EVENTS, "student-created")
            except Exception:
                await redis.lpush(QUEUE, payload)
                raise
    finally:
        await redis.close()
        await pool.close()


asyncio.run(run())