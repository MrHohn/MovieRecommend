from DataService import Mongo
import pymongo

class User(object):

    @classmethod
    def __init__(self, user_name, exist=False):
        self.name = user_name
        self.mongo = Mongo("movieRecommend")
        self.recommend = MovieRecommend(mongo)
        if not exist:
            # tag ids
            self.tags = set()
            # movie ids
            self.movies = set()
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
    def add_movie(self, movie):
        self.movies.add(movie)

    @classmethod
    def remove_movie(self, movie):
        if movie in self.movies:
            self.movies.remove(movie)

    # store user into database
    @classmethod
    def store_user(self):
        print("TODO")

    # store user into database
    @classmethod
    def load_user(self):
        print("TODO")

    @classmethod
    def get_recommend_by_tags(self):
        return self.recommend.recommend_movies_based_on_tags(self.tags)

    @classmethod
    def get_recommend_by_history(self):
        return self.recommend.get_similar_users_by_history(self.movies)
