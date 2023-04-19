
from memory import DatabaseStorage
from indexing.btree_index import Indexes
from crud.insert import Insert
from crud.update import Update
from crud.delete import Delete
import timeit
import random
from queries.queryhandler import QueryHandler

# Create a Driver license collection 1
col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')

# Insert 10000 records in collection 1
ins = Insert()

payloads = []
for i in range(10000):
    payload = {
        'name' : 'Peter',
        '_id' : str(i),
        'age' : random.randint(20,100)
    }
    payloads.append(payload)
ins.insert_many(database='database1', collection_name='collection1', payloads=payloads)

# # Create a drivers license collection 2
col2 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection2')

payloads = []
for i in range(10000):
    payload = {
        'name' : 'Demeter',
        '_id' : str(i),
        'age' : random.randint(20,100)
    }
    payloads.append(payload)
ins.insert_many(database='database1', collection_name='collection2', payloads=payloads)

print("Collections created")

# Create index for both collections 
# Create index for collection 1
indexes = Indexes()
col1_index = indexes.create_single_key_index(database='database1', collection='collection1', keys=['age'])

# Create index for collection 2
col2_index = indexes.create_single_key_index(database='database1', collection='collection2', keys=['age'])

print("Indexes created")

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

# Time the range search
start_time = timeit.default_timer()
query_result = indexes.range_search(database='database1', collection='collection1', keys = ['age'], start=25, end=60)
elapsed = timeit.default_timer() - start_time
print("Time for range search with index: {}".format(elapsed))

# print(query_result)

# Time the range search without index
start_time = timeit.default_timer()
# Get a query handler
query_handler = QueryHandler()

# Execute a find query for the range search
query_result = query_handler.handle_query('find', [
    {
        'age' : {
            'AND': [
                {
                    'leq': 60
                }, {
                    'geq': 25
                }
            ]
        }, 
    }, {'name' : 1, 'age': 1}
], col1.storage)

elapsed = timeit.default_timer() - start_time
print("Time for range search without index: {}".format(elapsed))

# print(query_result)