from pymongo import MongoClient

class Mongo(object):

    @classmethod
    def __init__(self, database):
        print("[mongo] Initializing database: [" + database + "]")
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[database]

    @classmethod
    def get_attr(self, collection, attrKey, filter_dict = {}):
        # return a list [attrValue]
        # return all data in the collection if no filter is specified
        result = []
        cursor = self.db[collection].find(filter_dict)
        for element in cursor:
            result.append(element[attrKey])
        return result

    @classmethod
    def get_pair(self, collection, userKey, valueKey, filter_dict = {}):
        # return a dict {key : value}
        # return all data in the collection if no filter is specified
        result = {}
        cursor = self.db[collection].find(filter_dict)
        for pair in cursor:
            result[pair[userKey]] = pair[valueKey]
        return result

    @classmethod
    def insert_one(self, collection, doc):
        insert_id = self.db[collection].insert_one(doc).inserted_id

    @classmethod
    def find_one(self, collection, filter_dict):
        return self.db[collection].find_one(filter_dict)

    @classmethod
    def replace_one(self, collection, filter_dict, doc):
        return self.db[collection].replace_one(filter_dict, doc)

    @classmethod
    def get_collection(self, collection):
        return self.db[collection]

    @classmethod
    def __del__(self):
        print("[mongo] Closing all connections")
        # clean up work
        if "client" in locals():
            self.client.close()