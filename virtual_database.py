from typing import Iterable
import mysql.connector
from cachetools import LRUCache
from utils import binary_search


class VirtualRow:
    def __init__(self, word: str, freq: str, polarity: float, subjectivity: float) -> None:
        self.word = word
        self.freq = freq
        self.polarity = polarity
        self.subjectivity = subjectivity
        
    def get_key(self) -> str:
        return self.word
    
    def get_freq(self) -> int:
        return self.freq
    
    def get_polarity(self) -> int:
        return self.polarity
    
    def get_subjectivity(self) -> int:
        return self.subjectivity
    
    def set_freq(self, freq: int) -> None:
        self.freq = freq
    
    def set_subjectivity(self, subjectivity: float) -> None:
        self.subjectivity = subjectivity
    
    def set_polarity(self, polarity: float) -> None:
        self.polarity = polarity
    
    def update_freq(self, add: int) -> None:
        self.freq += add        
    
    def update_polarity(self, add: float) -> None:
        self.polarity += add
        self.polarity /= 2
    
    def update_subjectivity(self, add: float) -> None:
        self.subjectivity += add
        self.subjectivity /= 2
        
    def __repr__(self) -> str:
        return f"Row({self.get_key()}, {self.get_freq()}, {self.get_polarity()}, {self.get_subjectivity()})"
    
    
class VirtualTable:
    def __init__(self, name: str) -> None:
        self.rows = LRUCache(maxsize=250)
        self.name = name
        
    def get_name(self) -> str:
        return self.name
        
    def add_row(self, row: VirtualRow) -> None:
        self.rows[row.get_key()] = row
    
    def add_rows(self, *rows: Iterable[VirtualRow]) -> None:
        for row in rows:
            self.add_row(row)
    
    def get_row(self, key: str) -> VirtualRow:
        if row := self.rows.get(key):
            return row
    
    def get_rows(self, *keys: Iterable[str]) -> Iterable[VirtualRow]:
        if len(keys) == 0:
            keys = self.rows.keys()
            
        for key in keys:
            yield self.get_row(key)
            
    def has_row(self, key: str) -> bool:
        if key in self.rows:
            return True
        else: 
            return False
            
    def set_row(self, key: str, freq: int = None, polarity: float = None, subjectivity: float = None) -> None:
        if not self.has_row(key):
            print(f"Warning: Key '{key}' Not Found In Table {self.name}!")
            return
        
        if freq is not None:
            self.rows[key].freq = freq
        if polarity is not None:
            self.rows[key].polarity = polarity
        if subjectivity is not None:
            self.rows[key].subjectivity = subjectivity
    
    def update_row(self, key: str, freq: int = None, polarity: float = None, subjectivity: float = None) -> None:
        if not self.has_row(key):
            print(f"Warning: Key '{key}' Not Found In Table {self.name}!")
            return
        
        if freq is not None:
            self.rows[key].freq += freq
        if polarity is not None:
            self.rows[key].polarity += polarity
            self.rows[key].polarity /= 2
        if subjectivity is not None:
            self.rows[key].subjectivity = subjectivity
            self.rows[key].subjectivity /= 2
            
    def __repr__(self) -> str:
        return f"Table(name={self.name}, " + ", ".join([row.__repr__() for row in self.get_rows()]) + ")"


class VirtualDataBase:
    def __init__(self) -> None:
        self.tables: dict[str, VirtualTable] = {}
        
    def get_table(self, name: str) -> VirtualTable:
        return self.tables.get(name, None)
    
    def get_tables(self) -> VirtualTable:
        for table in self.tables.values():
            yield table
    
    def get_row(self, table: str, key: str) -> VirtualRow:
        return self.get_table(table).get_row[key]
    
    def has_table(self, name: str) -> bool:
        """table_names = list(self.tables.keys())
        result = binary_search(table_names, name)
        if result is True:
            return True
        else:
            return False"""
        return name in self.tables.keys()
    
    def has_row(self, table: str, key: str) -> bool:
        return self.get_table(table).has_row(key)

    def add_table(self, table: VirtualTable) -> None:
        self.tables[table.get_name()] = table
    
    def create_table(self, name: str) -> None:
        self.add_table(VirtualTable(name))
        
    def insert_row(self, table: str, row: VirtualRow) -> None:
        self.tables[table].add_row(row)
    
    def insert_rows(self, table: str, *rows: Iterable[VirtualRow]) -> None:
        self.tables[table].add_rows(rows)
    
    def set_row(self, table: str, key: str, freq: int = None, polarity: float = None, subjectivity: float = None) -> None:
        self.get_table[table].set_row(key, freq, polarity, subjectivity)
    
    def update_row(self, table: str, key: str, freq: int = None, polarity: float = None, subjectivity: float = None) -> None:
        self.tables[table].update_row(key, freq, polarity, subjectivity)
        
    def sync_to_database(self, db: mysql.connector.MySQLConnection) -> None:
        """
        Uploads data in virtual db to real db
        """ 
        cursor = db.cursor()
        for table in self.tables:
            cursor.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = %s", (table, ))
            if cursor.fetchone() is None:
                cursor.execute(f"CREATE TABLE {table} (word VARCHAR(30), frequency bigint, polarity float, subjectivity float)")

            for row in self.tables[table].get_rows():
                cursor.execute(f"SELECT 1 FROM {table} WHERE word=%s;", (row.get_key(), ))
                
                if cursor.fetchone() is None:
                    cursor.execute(f"INSERT INTO {table} (word, frequency, polarity, subjectivity) VALUES (%s, %s, %s, %s)", (row.get_key(), row.get_freq(), row.get_polarity(), row.get_subjectivity()))
                else:          
                    cursor.execute(f"UPDATE {table} SET frequency = {row.get_freq()}, polarity = {row.get_polarity()}, subjectivity = {row.get_subjectivity()} WHERE word='{row.get_key()}'")
         
        cursor.close()
    
    def sync_from_database(self, db: mysql.connector.MySQLConnection, db_name: str) -> None:
        """
        Downloads data from real db to virtual db
        """
        cursor = db.cursor()
        
        cursor.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{db_name}'")
        tables = [row[0] for row in cursor]
        for table in tables:
            cursor.execute(f"SELECT * FROM {table}")
            for row in cursor:
                if not self.has_table(table):
                    self.create_table(table)
                    
                if self.get_table(table).has_row(row[0]):
                    self.get_table(table).set_row(
                        row[0],
                        row[1],
                        row[2],
                        row[3]
                    )
                else:
                    self.get_table(table).add_row(
                        VirtualRow(
                            row[0],
                            row[1],
                            row[2],
                            row[3]   
                        )
                    )
        
        cursor.close() 
    
    def __repr__(self) -> str:
        return f"DataBase(" + "\n".join([self.tables[table].__repr__() for table in self.tables.keys()]) + ")"
    