from memory import DatabaseStorage
import random
import string
from queries.queryhandler import QueryHandler
from crud.insert import Insert
from crud.delete import Delete
from crud.update import Update
from chunks import constants
from chunks.chunkify import Chunkify

def construct_payload(n: int):
    i = 0
    while i < n:
        payload = {
            "name": ''.join(random.choices(string.ascii_letters, k=7)),
            "address": ''.join(random.choices(string.ascii_letters + string.digits, k=10)),
            "massive payload": ''.join(random.choices(string.ascii_letters, k=10)),
            "dummy_key": ''.join(random.choices(string.ascii_letters, k=20))
        }
        yield payload
        i += 1

db = DatabaseStorage(database_name='final_db', collection_name='c_test_0')

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
for payload in construct_payload(5):
    ins.insert_one(database='final_db', collection_name='c_test_0', payload=payload)

# payload = {
#     'name': 'Beerus Sama222',
#     'age': 100,
#     'occupation': 'God of Destruction',
#     'hobbies': [
#         'eating', 'sleeping', 'hakai'
#     ]
#     # "_id" : "890412389051298591239"
# }

# payload_arr = []
# for i in range(3):
#     payload_arr.append({"X": i, "Y": i + 2})

# for i in range(3):
#     # payload['age'] = i
#     # payload["_id"] = str(int(payload["_id"]) + 1 ) + ""
#     # print ("Record ", payload, " in progress")
#     # ins.insert_one(database='final_db', collection_name='c_test_0', payload=payload)
#     ins.insert_one(database='final_db', collection_name='c_test_0', payload=payload_arr[i])



# chunkz = Chunkify(database_name = "database1_test_gb", collection_name = "collection_test")

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