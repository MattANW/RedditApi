import praw
import asyncio
import mysql.connector
from collections import deque
from typing import List, Any
from comment_data import CommentData
from virtual_database import VirtualDataBase, VirtualRow
from utils import is_english, filter_characters


async def DataMiner(reddit: praw.reddit.Reddit, db: mysql.connector.MySQLConnection, vdb: VirtualDataBase) -> None:

    queue = deque()
    mutex = asyncio.Lock()
    
    async def sync_db() -> None:
        while True:
            print("Syncing to database!")
            vdb.sync_to_database(db)
            
            await asyncio.sleep(5)

    async def comment_handler(comment: Any) -> None:
        # todo: more sophisticated text filter module
        text = comment.body
        
        if "bot" in text:
            return
        if not is_english(text):
            return
        
        post_id = comment.submission.id
        community_name = filter_characters(comment.subreddit.display_name)
        community_name = f"T{community_name}"
 
        comment_data = CommentData(
                    comment=text,
                    community_id=community_name,
                    post=post_id,
                )

        if not comment_data.is_empty():
            async with mutex:
                queue.append(comment_data)

    async def comment_data_handler():
        # do stuff like handle comment data, i.e. process it add the statistics to broad community, post and reddit statistics about what people will buy... ps I already know Duncan wants dildoes that vibrate.
        while True:
            if len(queue) > 0:
                new_comment_data: CommentData = queue.pop()

                table = f"{new_comment_data._community_id}"
                if not vdb.has_table(table):
                    vdb.create_table(table)

                for word_data in new_comment_data.get_words():
                    if not vdb.has_row(table, word_data[0]):
                        vdb.insert_row(table, VirtualRow(*word_data))
                    else:
                        vdb.update_row(table, *word_data)

            await asyncio.sleep(0.001)

    asyncio.create_task(comment_data_handler())
    asyncio.create_task(sync_db())

    for new_comment in reddit.subreddit("all").stream.comments():
        asyncio.create_task(comment_handler(new_comment))

        await asyncio.sleep(0.001)
    
def export():
    pass

if __name__ == "__main__":
    reddit = praw.Reddit(
        check_for_async=False,
        client_id="pR19Z1FPJCntgpmdrDUE5g",
        client_secret="233GM4dMBbIF8ALlj0FfWEik2aJXSw",
        password="",
        user_agent="MSi2DaBot",
        username="ThisIsATestTime",
    )

    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="$^7ijyecCtRgnauucFNJ",
        database="hamburgersever"
    )
    
    dbcursor = db.cursor()

    vdb = VirtualDataBase()
    vdb.sync_from_database(db, "hamburgersever")

    asyncio.run(DataMiner(reddit, db, vdb))
