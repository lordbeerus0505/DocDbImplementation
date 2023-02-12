"""
File handles insertOne and insertMany operations where we insert one entry to the database. 
Design decision on when to call write to disk is yet to be determined but at this point,
let it happen after each successful operation.
"""
import random
import string
from memory import DatabaseStorage

class Insert:
    def __init__(self) -> None:
        pass

    def insert_one(self, database, collection_name, payload, database_location = './'):
        # creates the database and the collection if they dont exist as well.
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        # Check for duplicates by Primary Key
        if "_id" not in payload:
            payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
        # Make sure the '_id' isnt in the database already, if it is, raise Exception
        for i,entry in enumerate(db.storage['_data']):
            if payload['_id'] == entry['_id']:
                raise Exception('Key Conflict, Alter the Primary Key and try again')
        if '_data' not in db.storage:
            raise Exception("Issues with the collection")
        db.storage['_data'].append(payload)
        db.write_file()
        return "OK"
    
    def insert_many(self, database, collection_name, payloads, database_location = './'):
        # creates the database and the collection if they dont exist as well.
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        if '_data' not in db.storage:
            raise Exception("Issues with the collection")
        for payload in payloads:
            if "_id" not in payload:
                payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
            # Make sure the '_id' isnt in the database already, if it is, raise Exception
            for i,entry in enumerate(db.storage['_data']):
                if payload['_id'] == entry['_id']:
                    raise Exception('Key Conflict, Alter the Primary Key and try again')
            db.storage['_data'].append(payload)
        db.write_file()
        return "OK"
