import json
from pymongo import MongoClient


class DatabaseManager:
    def __init__(self):
        self.client = MongoClient("localhost", 27017)
        self.database = self.client["mydatabase"]

    def create_table(self):
        pass

    def add_data(self, collection_name, **kwargs):
        self.database[collection_name].insert_one(kwargs)

    def get_existing_relations(self):
        result = self.database["student_advisor"].find()
        return [
            (
                i["student_id"],
                i["advisor_id"],
            )
            for i in result
        ]

    def delete_row(self, collection_name, row_id):
        if collection_name == "advisor":
            self.database[collection_name].delete_one({"advisor_id": row_id})
        else:
            self.database[collection_name].delete_one({"student_id": row_id})

    def load_data(self, collection_name):
        return list(self.database[collection_name].find())

    def search(self, collection_name, **kwargs):
        conditions = {}
        for key, value in kwargs.items():
            if value:
                conditions[key] = {"$regex": f".*{value}.*"}

        if conditions:
            return list(self.database[collection_name].find(conditions))
        else:
            return self.load_data(collection_name)

    def update(self, collection_name, name, surname, age, id):
        update_query = {"$set": {"name": name, "surname": surname, "age": age}}
        if collection_name == "students":
            self.database[collection_name].update_one({"student_id": id}, update_query)
        elif collection_name == "advisors":
            self.database[collection_name].update_one({"advisor_id": id}, update_query)

    def check_bd(self):
        return self.database["student_advisor"].count_documents({}) == 0

    def list_advisors_with_students_count(self, order_by):
        pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "advisor_id",
                    "foreignField": "advisor_id",
                    "as": "students",
                }
            },
            {
                "$group": {
                    "_id": "$advisor_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "student_count": {"$sum": {"$size": "$students"}},
                }
            },
            {"$sort": {"student_count": order_by}},
        ]
        return list(self.database["advisors"].aggregate(pipeline))

    def list_students_with_advisors_count(self, order_by):
        pipeline = [
            {
                "$lookup": {
                    "from": "student_advisor",
                    "localField": "student_id",
                    "foreignField": "student_id",
                    "as": "advisors",
                }
            },
            {
                "$group": {
                    "_id": "$student_id",
                    "name": {"$first": "$name"},
                    "surname": {"$first": "$surname"},
                    "advisor_count": {"$sum": {"$size": "$advisors"}},
                }
            },
            {"$sort": {"advisor_count": order_by}},
        ]
        return list(self.database["students"].aggregate(pipeline))

    def init_data_in_db(self, file_path="Data\\data.json"):
        with open(file_path, "r") as f:
            data = json.load(f)
            for collection_name, documents in data.items():
                for doc in documents:
                    self.add_data(collection_name, **doc)
