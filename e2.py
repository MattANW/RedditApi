import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from virtual_database import *
import mysql.connector
from typing import Tuple


def calculate_topic_popularity(vdb: VirtualDataBase, topic: str) -> Tuple[str, str]:
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
        
        


if __name__ == "__main__":
    db = mysql.connector.connect(
        host="Example",
        user="Example",
        password="Example",
        database="Example"
    )
    
    vdb = VirtualDataBase()
    vdb.sync_from_database(db, "Example")
    
    while True:
        topic = input("Enter a topic you would like data on: ")
        brief, extended_data = calculate_topic_popularity(vdb, topic)
        show_extended = input(f"{brief} Would you like extended data? y/n").lower()
        if show_extended == "y":
            print(extended_data)
        
    
