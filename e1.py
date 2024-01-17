import matplotlib.pyplot as plt
import numpy as np
from sklearn.cluster import KMeans
from virtual_database import *
import mysql.connector

def get_cmap(n, name='hsv'):
    return plt.cm.get_cmap(name, n)

if __name__ == "__main__":
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="$^7ijyecCtRgnauucFNJ",
        database="hamburgersever"
    )
    
    vdb = VirtualDataBase()
    vdb.sync_from_database(db, "hamburgersever")
    
    """# frequency, polarity and subjectivity
    freq = []
    pola = []
    subj = []
    
    for table in vdb.get_tables():
        for row in table.get_rows():
            freq.append(row.get_freq())
            pola.append(row.get_polarity())
            subj.append(row.get_subjectivity())
            
    fig = plt.figure()
    ax = plt.axes(projection ='3d')
    
    ax.scatter(freq, pola, subj)
    ax.set_xlabel("Frequency")
    ax.set_ylabel("Polarity")
    ax.set_zlabel("Subjectivity")
    plt.show()"""




    pola = []
    subj = []

    for table in vdb.get_tables():
        for row in table.get_rows():
            pola.append(row.get_polarity())
            subj.append(row.get_subjectivity())

    data = np.column_stack((pola, subj))

    num_clusters = 10

    kmeans = KMeans(n_clusters=num_clusters)
    kmeans.fit(data)
    
    labels = kmeans.labels_
    centers = kmeans.cluster_centers_

    fig = plt.figure()

    plt.scatter(data[:, 0], data[:, 1], c = labels)
    plt.scatter(centers[:, 0], centers[:, 1], marker = 'x', c = 'red')
    plt.xlabel("Polarity")
    plt.ylabel("Subjectivity")
    plt.title('ClusterisedData')
    plt.show()
