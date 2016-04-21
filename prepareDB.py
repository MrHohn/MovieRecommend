from DataService import Mongo
from movieRecommend import MovieRecommend
import pymongo
import movieLensParser
import time

def prepare_genres(mongo):
    print("[prepare_genres] Starting pepare genres list...")
    startTime = time.time()

    genres_dict = {}
    cursor = mongo.db["movie"].find({})
    for cur_movie in cursor:
        cur_mid = cur_movie["mid"]
        cur_genres = cur_movie["genres"]
        copy_movie = {}
        copy_movie["mid"] = cur_movie["mid"]
        copy_movie["imdb_rating"] = cur_movie["imdb_rating"]
        copy_movie["imdb_votes"] = cur_movie["imdb_votes"]
        for genre in cur_genres:
            if genre not in genres_dict:
                cur_list = []
            else:
                cur_list = genres_dict[genre]
            cur_list.append(copy_movie)
            genres_dict[genre] = cur_list

    for genre, movies in genres_dict.items():
        mongo.db["genres_list"].update_one({"genre": genre}, {"$set": {
            "relevant_movie": movies,
            "popular": len(movies)
            }}, True)
        # print(genre + " " + str(len(movies)))

    mongo.db["genres_list"].create_index([("genre", pymongo.ASCENDING)])
    print("[prepare_genres] Created index for genre in genres_list")
    print("[prepare_genres] Done (%0.2fs)." % (time.time() - startTime))

def prepare_actors(mongo):
    print("[prepare_actors] Starting pepare actors list...")
    startTime = time.time()

    actors_dict = {}
    cursor = mongo.db["movie"].find({})
    for cur_movie in cursor:
        cur_mid = cur_movie["mid"]
        cur_actors = cur_movie["actors"]
        copy_movie = {}
        copy_movie["mid"] = cur_movie["mid"]
        copy_movie["imdb_rating"] = cur_movie["imdb_rating"]
        copy_movie["imdb_votes"] = cur_movie["imdb_votes"]
        for actor in cur_actors:
            if actor not in actors_dict:
                cur_list = []
            else:
                cur_list = actors_dict[actor]
            cur_list.append(copy_movie)
            actors_dict[actor] = cur_list

    progressInterval = 3000    # How often should we print a progress report to the console?
    progressTotal = 55740      # Approximate number of total actors.
    bulkSize = 2000            # How many documents should we store in memory before inserting them into the database in bulk?
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["actors_list"], ordered = False)
    count = 0
    skipCount = 0
    for actor, movies in actors_dict.items():
        count += 1
        if count % progressInterval == 0:
            print("[prepare_actors] %5d actors processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        bulkPayload.find({"actor": actor}).update({"$set": {
            "relevant_movie": movies,
            "popular": len(movies)
            }})

        if count % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["actors_list"], ordered = False)

    if count % bulkSize > 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    mongo.db["actors_list"].create_index([("actor", pymongo.ASCENDING)])
    print("[prepare_actors] Created index for actor in actors_list")
    print("[prepare_actors] Skipped " + str(skipCount) + " insertions.")
    print("[prepare_actors] Done (%0.2fs)." % (time.time() - startTime))

def prepare_rankings(mongo):
    # top rated movies

    # most popular movies

    # top rated movies for each genres

    # most popular movies for each genres

    # most popular actors

    print("TODO")

def prepare_recommendations(mongo):
    print("TODO")

def prepare():
    print("[prepareDB] Starting pepare database...")
    startTime = time.time()

    mongo = Mongo("movieRecommend")

    movieLensParser.parse(mongo)
    prepare_genres(mongo)
    prepare_actors(mongo)
    prepare_rankings(mongo)
    prepare_recommendations(mongo)

    print("[prepareDB] Done (%0.2fs)." % (time.time() - startTime))


def main():
    prepare()

if __name__ == "__main__":
    main()