"""
Indexes: Use to create single key BTree inxed. Multi key index functionality to be added in next Phase.

BTreeIndex: Uses BTree libray to create a BTree index.
"""
from BTrees.OOBTree import OOBTree
from memory import DatabaseStorage
from rtree import index
import pickle

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

        # Insert the index into the indexes dictionary
        if database in self.indexes:
            if collection in self.indexes[database]:
                self.indexes[database][collection].update({tuple(keys): index})
            else:
                self.indexes[database][collection] = {tuple(keys): index}
        else:
            self.indexes[database] = {collection: {tuple(keys): index}}

        return index

    def create_multi_key_index(self, database, collection, keys):
        # Index name is the concatenation of all the keys and the database and collection 
        dimensions = None
        index_name = database + collection
        for key in keys:
            index_name += key
        if len(keys) == 2:
            dimensions = 2
        elif len(keys) == 3:
            dimensions = 3
        else:
            raise Exception('Only 2D and 3D indexes are supported')
        
        index = RTreeIndex(index_name, dimensions)
        index.create_multi_key_index(database, collection, keys)

        # Insert the index into the indexes dictionary
        if database in self.indexes:
            if collection in self.indexes[database]:
                self.indexes[database][collection].update({tuple(keys): index})
            else:
                self.indexes[database][collection] = {tuple(keys): index}
        else:
            self.indexes[database] = {collection: {tuple(keys): index}}

        return index

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
    
    def range_search(self, database, collection, keys, start, end):
        try:
            # indexes = self.indexes[database][collection]
            # for index in indexes:
            #     if index == tuple(keys):
            #         return indexes[index].index.values(start, end)
            index_name = database + collection
            for key in keys:
                index_name = index_name + key
            index_name = index_name + '.idx'

            file = open(index_name, 'rb')
            index = pickle.load(file)
            file.close()
            return index.values(start, end)

        except Exception as err:
            print(err)
            return None
    
    """
    Does a lazy update of all the indexes
    """
    def lazy_update(self):
        for database in self.indexes:
            for collection in self.indexes[database]:
                for index in self.indexes[database][collection]:
                    curr_index = self.indexes[database][collection][index]
                    # Update the index from the memory to the disk
                    index_name = curr_index.index_name
                    file = open(index_name, 'wb')
                    pickle.dump(curr_index, file)
                    file.close()

    """
    coords: list or tuple of 2 or 3 values used as coords for 2D or 3D 
    k: The number of nearest neighbours to return
    """
    def nearest_neighbour(self, database, collection, keys, coords, k=1):
        try:
            # Construct the index name
            index_name = database + collection
            for key in keys:
                index_name = index_name + key
            # index_name = index_name + '.idx'

            # Open the index file
            file_idx = index.Rtree(index_name)
            
            nearest = [n.object for n in file_idx.nearest(coords, k, objects=True)]
            return nearest
        except Exception as err:
            print(err)
            return None
    
    # Range search but for 2D and 3D bounds
    # Bounds is a Dimensions * 2 array specifying (left, bottom, right top) bounds
    def range_search_d(self, database, collection, keys, bounds):
        try:
            # Construct the index name
            index_name = database + collection
            for key in keys:
                index_name = index_name + key
            # index_name = index_name + '.idx'

            # Open the index file
            file_idx = index.Rtree(index_name)
            tuples = [n.object for n in file_idx.intersection(bounds, objects=True)]
            return tuples
        except Exception as err:
            print(err)
            return None

    
class BTreeIndex:
    def __init__(self):
        self.index = OOBTree()
        self.index_file = None
        self.index_name = None

    """ Given a collection file and the keys to use in index, create a BTree index"""
    def create_single_key_index(self, database, collection, index_key):
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection)
        coll = db.storage['_data']
        for key, value in coll.items():
            # Update the BTree index
            if index_key in value:
                self.index.update({value[index_key]: value})

        # Create a persistent index storage file on disk
        self.index_name = database + collection + index_key + '.idx'
        self.index_file = open(database + collection + index_key + '.idx', 'wb')
        pickle.dump(self.index, self.index_file)
        self.index_file.close()

    # Update the index - Currently supports a single key update only
    def update_index(self, values, keys):
        self.index.update({values[0]: keys[0]}) 

"""
RTree Index class
Multi-attribute indexing
Persistent RTree index
"""
class RTreeIndex:
    def __init__(self, name, dimensions):
        # Set the index properties
        self.p = index.Property()
        self.p.dat_extension = 'dat'
        self.p.idx_extension = 'idx'
        self.p.dimension = dimensions
        
        self.dimensions = dimensions

        self.index = index.Rtree(name, properties=self.p)

    """ Given a collection file and the keys to use in index, create a serialized RTree index on a disk file"""
    def create_multi_key_index(self, database, collection, index_keys):
        db = DatabaseStorage(database_location= database_location, database_name= database, collection_name= collection)
        coll = db.storage['_data']
        for key, value in coll.items():
            # Update the RTree index
            index_array = []
            for index_key in index_keys:
                index_array.append(value[index_key])
            
            id = value['id']
            if self.dimensions == 2:
                spatial_coords = [float(index_array[0]), float(index_array[1]), float(index_array[0]), float(index_array[1])]
                print(spatial_coords)
                self.index.insert(id=id, coordinates=spatial_coords, obj=value)
            if self.dimensions == 3:
                spatial_coords = [float(index_array[0]), float(index_array[1]), float(index_array[2]), float(index_array[0]), float(index_array[1]), float(index_array[2])]
                self.index.insert(id=id, coordinates=spatial_coords, obj=value)

    # Update the index
    def update_index(self, values, keys):
        pass
                    

