import asyncpraw as praw
import asyncio
from praw.models import MoreComments
from collections import deque
from typing import Any
from time import time
from comment_data import CommentData

counter = 0

reddit = praw.Reddit(
    client_id="Example",
    client_secret="Example",
    password="Example",
    user_agent="Example",
    username="Example",
)

async def DataMiner() -> None:

    queue = deque()
    mutex = asyncio.Lock()

    begin = time()

    async def comment_stream_handler(comment: Any) -> None:
        text = comment.body
        if "bot" in text:
            return
        
        post_id = comment.submission.id
        community_id = comment.subreddit_id

        comment_data = CommentData(
                    comment=text,
                    community=community_id,
                    post=post_id,
                )

        async with mutex:
            queue.append(comment_data)

    async def comment_data_handler():
        global counter
        # do stuff like handle comment data, i.e. process it add the statistics to broad community, post and reddit statistics about what people will buy... ps I already know Duncan wants dildoes that vibrate.
        while True:
            if len(queue) > 0:
                new_comment_data = queue.pop()
                counter += 1
                print(str(new_comment_data))
            await asyncio.sleep(0.001)

    asyncio.create_task(comment_data_handler())

    subreddit = await reddit.subreddit("all")
    generator = subreddit.stream.comments()

    while True:
        new_comment = await anext(generator)

        asyncio.create_task(comment_stream_handler(new_comment))

        finish = time()

        duration = finish - begin
        if duration >= 30:
            comments_per_second = counter / duration

            print(f"Comments per second (avg): {comments_per_second}!")

            begin = time()

        await asyncio.sleep(0.001)

if __name__ == "__main__":
    asyncio.run(DataMiner())
