from memory import DatabaseStorage
import random
import string
from queries.queryhandler import QueryHandler
from crud.insert import Insert
from crud.delete import Delete
from crud.update import Update

db = DatabaseStorage(database_name='database1', collection_name='collection1')
# import pdb; pdb.set_trace()
# id = ''.join(random.choices(string.ascii_letters + string.digits, k=38))
# db.storage['_data'][id]= {
#     "_id": id,
#     'new pikaa' : 'squirtle',
#     'pokemon_names': {"pikachu": "meoww", "testpoke": "pokemon123"}
# }
# db.write_file()

# Find Query
# q = QueryHandler()
# q.handle_query('find', [
#     ({'new pikaa' : {
#                     "eq" : "squirtle"
#                 }
#     }),
#     {'new pikaa' : 1, 'pokemon_names': 1}
# ], db.storage)


# Insert One and Insert Many
ins = Insert()
payload = {
    'name': 'Beerus Sama222',
    'age': 100,
    'occupation': 'God of Destruction',
    'hobbies': [
        'eating', 'sleeping', 'hakai'
    ]
}
ins.insert_one(database='database2', collection_name='new_collection', payload=payload)
# ins.insert_many(database_location='/Users/abhiram', database='database3', collection_name='dragonball_trivia', payloads=payloads)
# delete = Delete()
# delete.delete_one(database='database2', collection_name='new_collection', payload={'_id': 'tmszkn4WoUrJcvX62zxJzDzkOyT0Y8zhcHDovk'})

# update = Update()
# payload = {
#     "name": "Trunks",
#     "age": 18,
#     "occupation": "Saiyan Warrior",
#     "hobbies": [
#         "studying",
#         "training"
#     ],
#     "_id": "KDgLix3WwO2lEU8YdjIldtk6T5I6Le18NDmabx"
# }
# update.update_one(database='database2', collection_name='new_collection', payload=payload)