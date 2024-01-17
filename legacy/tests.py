import praw
import asyncio
import mysql.connector
from collections import deque
from typing import List, Any
from time import time
from comment_data import CommentData

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="$^7ijyecCtRgnauucFNJ",
    database="HamburgerSever"
    )

vb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="$^7ijyecCtRgnauucFNJ",
    database="testdatabase"
)

mycursorvb = vb.cursor()
mycursor = db.cursor()

def Datatransfer():
    mycursorvb.execute("SHOW TABLES")
    tables = mycursorvb.fetchall()

    #Transfer VB table data into DB
    for table in tables:
        table_name = table[0]

        # Retrieve column names and types from the source table
        mycursorvb.execute(f"SHOW COLUMNS FROM {table_name}")
        columns = mycursorvb.fetchall()
        
        create_table_query = f"CREATE TABLE `{table_name}` ("
        for column in columns:
            column_name = column[0]
            data_type = column[1].decode('utf-8')
            create_table_query += f"`{column_name}` {data_type}, "
        create_table_query = create_table_query.rstrip(", ")
        create_table_query += ")"
        mycursor.execute(create_table_query)

        mycursorvb.execute(f"SELECT * FROM {table_name}")
        rows = mycursorvb.fetchall()

        insert_query = f"INSERT INTO `{table_name}` VALUES ({','.join(['%s'] * len(rows[0]))})"
        mycursor.executemany(insert_query, rows)
            
    db.commit()
    
counter = 0

reddit = praw.Reddit(
    check_for_async=False,
    client_id="pR19Z1FPJCntgpmdrDUE5g",
    client_secret="233GM4dMBbIF8ALlj0FfWEik2aJXSw",
    password="",
    user_agent="MSi2DaBot",
    username="ThisIsATestTime",
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
    for new_comment in reddit.subreddit("all").stream.comments():
        asyncio.create_task(comment_stream_handler(new_comment))

        finish = time()

        duration = finish - begin
        if duration >= 15:
            comments_per_second = counter / duration

            print(f"Comments per second (avg): {comments_per_second}!")

            begin = time()

            transfer = Datatransfer()
            transfer()
            
            """for x in mycursor:
                print(x)"""

            #raise Exception("HI BUDDY!")

        await asyncio.sleep(0.001)

if __name__ == "__main__":
    asyncio.run(DataMiner())
