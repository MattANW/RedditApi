from fastapi import FastAPI
from virtual_database import *
import mysql.connector
import asyncio


def calculate_topic_popularity(vdb: VirtualDataBase, topic: str):
    community_data = {}
    
    # N is total num of communities while n is number who mention
    N = 0
    n = 0
    
    for table in vdb.get_tables():
        n_mentions = 0
        
        polarity = 0
        subjectivity = 0
        avg_divisor = 0
        
        for row in table.get_rows():
            if topic in row.get_key():
                n_mentions += row.get_freq()
                polarity += row.get_polarity()
                subjectivity += row.get_subjectivity()
                avg_divisor += 1
        if n_mentions > 0:
            n += 1
            community_data[table.get_name()] = {"mentions": n_mentions, "polarity": polarity / avg_divisor, "subjectivity": subjectivity / avg_divisor}
        N += 1
        
    return f"{n}/{N} communities mentioned topic {topic}", f"Extended data is: " + "\n".join([key + ": " + str(community_data[key]) for key in community_data])

app = FastAPI()
vdb = None


@app.on_event("startup")
def app_startup():
    global vdb
    
    db = mysql.connector.connect(
        host="Example",
        user="Example",
        password="Example",
        database="Example"
    )
    
    vdb = VirtualDataBase()
    
    def sync_local_db():
        vdb.sync_from_database(db, "Example")
        asyncio.sleep(3000)
    
    asyncio.new_event_loop()


@app.get("/get_topic_data")
def get_topic_data(topic: str):
    return calculate_topic_popularity(vdb, topic)[0]

@app.get("/get_extended_topic_data")
def get_topic_data(topic: str):
    return calculate_topic_popularity(vdb, topic)[1]
