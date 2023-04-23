from memory import DatabaseStorage
from indexing.btree_index import Indexes
from crud.insert import Insert
from crud.update import Update
from crud.delete import Delete
import timeit
import random
from queries.queryhandler import QueryHandler
from rtree import index
import math
import pickle 
import sys
sys.setrecursionlimit(10000)

# # Create a Driver license collection 1
# col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')

# # Insert 100000 records in collection 1
# ins = Insert()

# payloads = []
# for i in range(1000000):
#     payload = {
#         'name' : 'Peter',
#         '_id' : str(i),
#         'age' : random.randint(20,100),
#         'license_number': i+1000000,
#         'location_latitude': random.randint(0,100),
#         'location_longitude': random.randint(0,100),
#         'id': i
#     }
#     payloads.append(payload)
# ins.insert_many(database='database1', collection_name='collection1', payloads=payloads)

# # # Create a drivers license collection 2
# col2 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection2')

# payloads = []
# for i in range(1000000):
#     payload = {
#         'name' : 'Demeter',
#         '_id' : str(i),
#         'age' : random.randint(20,100),
#         'license_number': i+2000000,
#         'location_latitude': random.randint(0,100),
#         'location_longitude': random.randint(0,100),
#         'id': i
#     }
#     payloads.append(payload)
# ins.insert_many(database='database1', collection_name='collection2', payloads=payloads)

# print("Collections created")

# Create index for both collections 
# Create index for collection 1
indexes = Indexes()
col1_index = indexes.create_single_key_index(database='database1', collection='collection1', keys=['license_number'])

# Create index for collection 2
col2_index = indexes.create_single_key_index(database='database1', collection='collection2', keys=['license_number'])

print("Indexes created")

# Create index for both collections 
# Create index for collection 1
# indexes = Indexes()
# col1_index = indexes.create_multi_key_index(database='database1', collection='collection1', keys=['location_latitude', 'location_longitude'])

# # Create index for collection 2
# col2_index = indexes.create_multi_key_index(database='database1', collection='collection2', keys=['location_latitude', 'location_longitude'])

# print("Indexes created")

# # Insert a record with duplicate PK - Time with using indexing
# payload = {
#         'name' : 'Peter',
#         '_id' : '5000'
#     }

# start_time = timeit.default_timer()
# try:
#     ins.insert_one(database='database1', collection_name='collection1', payload=payload, pk_index=col1_index)
# except Exception as e:
#     print(e)
# elapsed = timeit.default_timer() - start_time
# print("Time for duplicate search with index: {}".format(elapsed))

# # Insert a record with duplicate PK - Time without using indexing
# payload = {
#         'name' : 'Peter',
#         '_id' : '5000'
#     }
# start_time = timeit.default_timer()
# try:
#     ins.insert_one(database='database1', collection_name='collection2', payload=payload)
# except Exception as e:
#     print(e)
# elapsed = timeit.default_timer() - start_time
# print("Time for duplicate search without index: {}".format(elapsed))

# # Insert a non duplicate and see changes reflected in the index
# payload = {
#         'name' : 'Peter',
#         '_id' : '10000'
#     }
# ins.insert_one(database='database1', collection_name='collection1', payload=payload, pk_index=col1_index)

# # Display all the indexes
# indexes.show_indexes('database1', 'collection1')

# col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database2', 'collection1')

# payload1 = {
#     "_id": "1",
#     "name": "rommel",
#     "blk_no": 12,
#     "street" : "dewey street",
#     "city" : "olongapo"
# }
# payload2 = {
#     "_id": "2",
#     "name": "gary",
#     "blk_no": 15,
#     "street" : "gordon street",
#     "city" : "olongapo"
# }
# ins = Insert()
# ins.insert_one(database='database2', collection_name='collection1', payload=payload1)
# ins.insert_one(database='database2', collection_name='collection1', payload=payload2)

# col2 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database2', 'collection2')

# payload1 = {
#     "_id": "1",
#     "contact_name": "rommel",
#     "age": 37,
#     "sex" : "male",
#     "citizenship" : "Filipino"
# }
# payload2 = {
#     "_id": "2",
#     "contact_name": "gary",
#     "age": 32,
#     "sex" : "male",
#     "citizenship" : "Filipino"
# }

# ins.insert_one(database='database2', collection_name='collection2', payload=payload1)
# ins.insert_one(database='database2', collection_name='collection2', payload=payload2)

# Range search query using "age" index on collection 1 - Find folks in age range (25, 60)

# # Time the range search
# indexes = Indexes()
# col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')

# start_time = timeit.default_timer()
# # Get index from pickle file
# file = open('database1collection1age.idx', 'rb')
# index = pickle.load(file)
# print(len(list(index.values())))

# query_result = indexes.range_search(database='database1', collection='collection1', keys = ['age'], start=25, end=60)
# elapsed = timeit.default_timer() - start_time
# print("Time for range search with index: {}".format(elapsed))

# print(len(list(query_result)))

# # Time the range search without index
# start_time = timeit.default_timer()
# # Get a query handler
# query_handler = QueryHandler()

# # Execute a find query for the range search
# query_result = query_handler.handle_query('find', [
#     {
#         'age' : {
#             'AND': [
#                 {
#                     'leq': 60
#                 }, {
#                     'geq': 25
#                 }
#             ]
#         }, 
#     }, {'name' : 1, 'age': 1}
# ], col1.storage, return_as_json=True)

# elapsed = timeit.default_timer() - start_time
# print("Time for range search without index: {}".format(elapsed))

# print(len(query_result))

# Multi attribute Rtree query
# file_idx = index.Rtree('database1collection1location_latitudelocation_longitude')
# nearest = [n.object for n in file_idx.nearest((10, 10, 10, 10), 1, objects=True)]
# print(nearest)

# # Multi attribute Rtree - Example 1 - Nearest neighbour
# indexes = Indexes()
# # Time the nearest neighbours search
# start_time = timeit.default_timer()
# nearest = indexes.nearest_neighbour(database='database1', collection='collection1', keys=['location_latitude', 'location_longitude'], coords=(10, 10), k=10)
# end_time = timeit.default_timer()
# elapsed = end_time - start_time
# print("Time for nearest neighbour search with index: {}".format(elapsed))
# # Print only the ids of the nearest objects
# print([item['_id'] for item in nearest])

# # Time for nearest neighbour search whout the index
# start_time = timeit.default_timer()

# # Open Collection 1 in Database 1
# col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')
# data = col1.storage['_data']

# # Store lat, long and _id in a list as a tuple
# input_list = []
# for key, value in data.items():
#     input_list.append((data[key]['_id'], data[key]['location_latitude'], data[key]['location_longitude']))

# # Find the distance of each point from the given point - (O(n))
# coords = (10, 10)
# distances = []
# for item in input_list:
#     distances.append((item[0], math.sqrt((item[1] - coords[0])**2 + (item[2] - coords[1])**2)))

# # Sort the list based on the distance - (O(nlogn))
# distances.sort(key=lambda x: x[1])

# # Return the first 10 elements
# nearest_ids = distances[:14]
# # Get the nearest objects from the IDs and data
# nearest = []
# for item in nearest_ids:
#     nearest.append(data[item[0]])

# end_time = timeit.default_timer()
# elapsed = end_time - start_time
# print("Time for nearest neighbour search without index: {}".format(elapsed))
# print(list([item[0] for item in nearest_ids]))

# # Multi attribute Rtree - Example 2 range search 2D
# # Time the range search
# indexes = Indexes()
# start_time = timeit.default_timer()
# bounds = (10, 10, 20, 20)
# query_result = indexes.range_search_d(database='database1', collection='collection1', keys = ['location_latitude', 'location_longitude'], bounds=bounds)
# elapsed = timeit.default_timer() - start_time
# print("Time for range search with index: {}".format(elapsed))
# # Print only the ids of the query result
# print([item['_id'] for item in query_result])

# # Range search without index
# start_time = timeit.default_timer()
# # Open Collection 1 in Database 1
# col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')
# data = col1.storage['_data']

# # Store lat, long and _id in a list as a tuple
# input_list = []
# for key, value in data.items():
#     input_list.append((data[key]['_id'], data[key]['location_latitude'], data[key]['location_longitude']))

# # Check if the point is within the bounds - O(n)
# query_result_ids = []
# for item in input_list:
#     if item[1] >= bounds[0] and item[1] <= bounds[2] and item[2] >= bounds[1] and item[2] <= bounds[3]:
#         query_result_ids.append(item[0])

# end_time = timeit.default_timer()
# elapsed = end_time - start_time
# print("Time for range search without index: {}".format(elapsed))

# # Get the query results from the IDs
# query_result = []
# for item in query_result_ids:
#     query_result.append(data[item])

# print(query_result_ids)