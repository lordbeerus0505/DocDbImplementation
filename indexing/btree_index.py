"""
Indexes: Use to create single key BTree inxed. Multi key index functionality to be added in next Phase.

BTreeIndex: Uses BTree libray to create a BTree index.
"""
from BTrees.OOBTree import OOBTree
from memory import DatabaseStorage

database_location = './'

# Class to create a global Index object
class IndexStorage:
    def __init__(self):
        self.indexes = Indexes()
    
    def get_index(self, database, collection, keys):
        return self.indexes.get_index(database, collection, keys)
    
    def create_index(self, database, collection, keys):
        return self.indexes.create_index(database, collection, keys)
    
    def create_multi_key_index(self, database, collection, keys):
        return self.indexes.create_multi_key_index(database, collection, keys)
    
    def delete_index(self, database, collection, keys):
        return self.indexes.delete_index(database, collection, keys)
    
    def show_indexes(self, database = None, collection = None):
        self.indexes.show_indexes(database, collection)

class Indexes:
    # indexes contains all the indexes for each database+collection pair
    def __init__(self):
        self.indexes = {}

    def create_single_key_index(self, database, collection, keys):
        index = BTreeIndex()
        index.create_single_key_index(database, collection, keys[0])

        # Inset the index into the indexes dictionary
        if database in self.indexes:
            if collection in self.indexes[database]:
                self.indexes[database][collection].update({tuple(keys): index})
            else:
                self.indexes[database][collection] = {tuple(keys): index}
        else:
            self.indexes[database] = {collection: {tuple(keys): index}}

        return index

    def create_multi_key_index(self, database, collection, keys):
        pass

    # Delete index for the given database and collection for the given key specification
    def delete_index(self, database, collection, keys):
        try:
            indexes = self.indexes[database][collection]
            for index in indexes:
                if index == tuple(keys):
                    indexes.remove(index)
                    break
        except Exception as err:
            print(err)
            
    # Print all the indxes for each collection in a database
    def show_indexes(self, database = None, collection = None):
        if database == None:
            # Display all the indexes from all databases
            for database in self.indexes:
                for collection in self.indexes[database]:
                    print(f'Indexes for {database}.{collection}')
                    for index in self.indexes[database][collection]:
                        #print(index, list(self.indexes[database][collection][index].index.keys()))
                        print(index)
        elif collection == None:
            # Display all the indexes for a given database
            for collection in self.indexes[database]:
                print(f'Indexes for {database}.{collection}')
                for index in self.indexes[database][collection]:
                    #print(index, list(self.indexes[database][collection][index].index.keys()))
                    print(index)
        else:
            # Display all the indexes for a given database and collection
            print(f'Indexes for {database}.{collection}')
            for index in self.indexes[database][collection]:
                # print(index, list(self.indexes[database][collection][index].index.keys()))
                print(index)
                
    def get_index(self, database, collection, keys):
        try:
            indexes = self.indexes[database][collection]
            for index in indexes:
                if index == tuple(keys):
                    return indexes[index]
        except Exception as err:
            print(err)
            return None
    
class BTreeIndex:
    def __init__(self):
        self.index = OOBTree()

    """ Given a collection file and the keys to use in index, create a BTree index"""
    def create_single_key_index(self, database, collection, index_key):
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection)
        coll = db.storage['_data']
        for key, value in coll.items():
            # Update the BTree index
            if index_key in value:
                self.index.update({value[index_key]: index_key})

    # Update the index - Currently supports a single key update only
    def update_index(self, values, keys):
        self.index.update({values[0]: keys[0]}) 
       

                    

