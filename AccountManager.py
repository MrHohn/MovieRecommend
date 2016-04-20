from DataService import Mongo
import pymongo

class User(object):

    @classmethod
    def __init__(self, user_name, exist=False):
        self.name = user_name
        if not exist:
            # tag ids
            self.tags = set()
            # movie ids
            self.like_movies = set()
        else:
            self.load_user()

    @classmethod
    def change_name(self, new_name):
        self.name = new_name

    @classmethod
    def add_tag(self, tag):
        self.tags.add(tag)

    @classmethod
    def remove_tag(self, tag):
        if tag in self.tags:
            self.tags.remove(tag)

    @classmethod
    def add_like(self, like):
        self.like_movies.add(like)

    @classmethod
    def remove_like(self, like):
        if like in self.like_movies:
            self.like_movies.remove(like)

    # store user into database
    @classmethod
    def store_user(self):
        print("TODO")

    # store user into database
    @classmethod
    def load_user(self):
        print("TODO")
