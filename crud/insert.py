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
    
    """
    Insertion operation can be made faster by using indexes to check for duplicates. pk_index is the primary key index. By default, it is the 
    index on _id. indexes is a list of indexes for this collection that are to be updated after the insertion except the pk_index.

    TODO: Check that that collection indeed does have those indexes
    """
    def insert_one(self, database, collection_name, payload, database_location = './', pk_index = None, indexes = None):
        # creates the database and the collection if they dont exist as well.
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)

        # Check if the collection exists
        if '_data' not in db.storage:
            raise Exception("Issues with the collection")
        
        # Check for duplicates using the primary key index
        if pk_index != None:
            if "_id" not in payload:
                payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
            # Make sure the '_id' isnt in the database already using index, if it is, raise Exception
            if pk_index.index.has_key(payload['_id']):
                raise Exception('Key Conflict, Alter the Primary Key and try again')
        else:
            # Check for duplicates by Primary Key
            if "_id" not in payload:
                payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
            # Make sure the '_id' isnt in the database already, if it is, raise Exception
            for i, id in enumerate(db.storage['_data']):
                if payload['_id'] == id:
                    raise Exception('Key Conflict, Alter the Primary Key and try again')
        db.storage['_data'][payload['_id']] = payload

        # Update the indexes - For now, updating the pk_index only
        if pk_index:
            pk_index.update_index([payload['_id']], ['_id'])
            
        db.write_file()
        return "OK"
    
    def insert_many(self, database, collection_name, payloads, database_location = './', pk_index = None, indexes = None):
        # creates the database and the collection if they dont exist as well.
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection_name)
        if '_data' not in db.storage:
            raise Exception("Issues with the collection")
        
        # Check for duplicates using the primary key index
        if pk_index != None:
            for payload in payloads:
                if "_id" not in payload:
                    payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
                # Make sure the '_id' isnt in the database already using index, if it is, raise Exception
                if pk_index.index.has_key(payload['_id']):
                    raise Exception('Key Conflict, Alter the Primary Key and try again')
                db.storage['_data'][payload['_id']] = payload
        else:
            for payload in payloads:
                if "_id" not in payload:
                    payload['_id']= ''.join(random.choices(string.ascii_letters + string.digits, k=38))
                # Make sure the '_id' isnt in the database already, if it is, raise Exception
                for i, id in enumerate(db.storage['_data']):
                    if payload['_id'] == id:
                        raise Exception('Key Conflict, Alter the Primary Key and try again')
                db.storage['_data'][payload['_id']] = payload

                # Update the indexes on each insert
                if pk_index:
                    pk_index.update_index([payload['_id']], ['_id'])

        db.write_file()
        return "OK"
