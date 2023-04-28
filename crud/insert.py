"""
File handles insertOne and insertMany operations where we insert one entry to the database. 
Design decision on when to call write to disk is yet to be determined but at this point,
let it happen after each successful operation.
"""
import random
import time
import string
import threading
from memory import DatabaseStorage

class Insert:
    def __init__(self, interval: int = 0) -> None:
        self.db = None
        self.start_commit = False
        self.interval = interval
        self.commitLock = threading.Lock()
        self.writeLock = threading.Lock()

    def writeToDisk(self):
        self.db.write_file()
    
    def stop_commit(self):
        self.runtime.cancel()
        self.db.write_file()
        self.start_commit = False
        print("Commit complete")

    def commit(self,):
        self.runtime = threading.Timer(self.interval, self.commit)
        while self.writeLock.locked():
            pass
        # Acquire the commit lock
        self.commitLock.acquire()
        self.runtime.start()
        print("committing...")
        self.writeToDisk()
        self.commitLock.release()

    def insert_one(self, database: str, collection_name: str, payload: dict, database_location: str = './', group_commit: bool = False):
        # Caching the database from the past
        if not group_commit:
            self.db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        elif (group_commit and not self.start_commit):
            # creates the database and the collection if they dont exist as well.
            self.db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
            self.start_commit = True
            self.commit()

        while self.commitLock.locked():
            pass
        # Acquire the write lock
        self.writeLock.acquire()
        # Check for duplicates by Primary Key
        if "_id" not in payload:
            payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
        # Make sure the '_id' isnt in the database already, if it is, raise Exception
        if '_data' not in self.db.storage:
            raise Exception("Issues with the collection")
        
        if payload['_id'] in self.db.storage['_data'].keys():
            raise Exception('Key Conflict, Alter the Primary Key and try again')
        
        self.db.storage['_data'][payload['_id']] = payload
        self.writeLock.release()
        if not group_commit:
            self.db.write_file()
            # self.db.write_file_chunk(database, collection_name, payload)
        return "OK"
    
    def insert_many(self, database: str, collection_name: str, payloads: dict, database_location: str = './', group_commit: bool = False):
        # creates the database and the collection if they dont exist as well.
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        if '_data' not in db.storage:
            raise Exception("Issues with the collection")

        for payload in payloads:
            if "_id" not in payload:
                payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
            # Make sure the '_id' isnt in the database already, if it is, raise Exception
            if '_data' not in self.db.storage:
                raise Exception("Issues with the collection")
            if payload['_id'] in self.db.storage['_data'].keys():
                raise Exception('Key Conflict, Alter the Primary Key and try again')
            db.storage['_data'][payload['_id']] = payload
        if not group_commit:
            db.write_file()

        return "OK"
