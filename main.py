from memory import DatabaseStorage
import random
import string
from queries.queryhandler import QueryHandler

db = DatabaseStorage('/Users/abhiram/Spring23/databases/project/DocDbImplementation', 'database1', 'collection1')
# import pdb; pdb.set_trace()
# db.storage['_data'].append({
#     "_id": ''.join(random.choices(string.ascii_letters + string.digits, k=38)),
#     'new pikaa' : 'squirtle',
#     'pokemon_names': {"pikachu": "meoww", "testpoke": "pokemon123"}
# })
# db.write_file()

q = QueryHandler()
q.handle_query('find', [
    ({'new pikaa' : {
                        'eq': 'squirtle'
                    }}),
    {'new pikaa' : 1, 'pokemon_names': 1}
], db.storage)
