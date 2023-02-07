from memory import DatabaseStorage
import random
import string
from queries.queryhandler import QueryHandler
from crud.insert import Insert

db = DatabaseStorage('/Users/abhiram/Spring23/databases/project/DocDbImplementation', 'database1', 'collection1')
# import pdb; pdb.set_trace()
# db.storage['_data'].append({
#     "_id": ''.join(random.choices(string.ascii_letters + string.digits, k=38)),
#     'new pikaa' : 'squirtle',
#     'pokemon_names': {"pikachu": "meoww", "testpoke": "pokemon123"}
# })
# db.write_file()

# Find Query
# q = QueryHandler()
# q.handle_query('find', [
#     ({'new pikaa' : {
#                     "OR" :[
#                         {
#                             'leq': 'cheekoo'
#                         }, 
#                         {   
#                             'gt' : 'tom'
#                         }
#                     ]
#                 }
#     }),
#     {'new pikaa' : 1, 'pokemon_names': 1}
# ], db.storage)


# Insert One and Insert Many
ins = Insert()
payloads = [{
    'name': 'Beerus Sama',
    'age': 10000,
    'occupation': 'God of Destruction',
    'hobbies': [
        'eating', 'sleeping', 'hakai'
    ]
}, {
    'name': 'Son Goku',
    'age': 55,
    'occupation': 'Fighter',
    'hobbies': [
        'kamehameha','fighting','eating'
    ]
}, {
    'name': 'Prince Vegeta',
    'age': 61,
    'occupation': 'Saiyan Prince',
    'hobbies': [
        'defeat kakarot', 'fighting', 'become the next GoD'
    ]
}
]
# ins.insert_one(database='database2', collection_name='new_collection', payload=payload)
ins.insert_many(database_location='/Users/abhiram', database='database3', collection_name='dragonball_trivia', payloads=payloads)