from Data.db import DatabaseManager

db_manager = DatabaseManager()
db_manager.init_data_in_db()

result = db_manager.search("students", name="Tinatin")
print(result)
