from memory import DatabaseStorage
import random
import string
from queries.queryhandler import QueryHandler
from crud.insert import Insert
from crud.delete import Delete
from crud.update import Update

col1 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection1')

col1.storage['_data'].append({
    "_id": "1",
    "name": "rommel",
    "blk_no": 12,
    "street" : "dewey street",
    "city" : "olongapo"
})
col1.storage['_data'].append({
    "_id": "2",
    "name": "gary",
    "blk_no": 15,
    "street" : "gordon street",
    "city" : "olongapo"
})
col1.write_file()

col2 = DatabaseStorage('/Users/sriramrao/Documents/Spring 2023/Databases/Project/DocDbImplementation', 'database1', 'collection2')

col2.storage['_data'].append({
    "_id": "1",
    "contact_name": "rommel",
    "age": 37,
    "sex" : "male",
    "citizenship" : "Filipino"
})
col2.storage['_data'].append({
    "_id": "2",
    "contact_name": "gary",
    "age": 32,
    "sex" : "male",
    "citizenship" : "Filipino"
})
col2.write_file()

qh = QueryHandler()
qh.handle_query('lookup', [col2.storage, 'name', 'contact_name', 'additional_deets'], col1.storage)