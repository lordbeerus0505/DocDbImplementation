from memory import DatabaseStorage

db = DatabaseStorage('/Users/abhiram/Spring23/databases/project/DocDbImplementation', 'database1', 'collection2')
db.storage['pokemon'] = {
    'new pikaa' : 'woof',
    'pokemon_names': {"pikachu": "meoww", "testpoke": "pokemon123"}
}
db.write_file()