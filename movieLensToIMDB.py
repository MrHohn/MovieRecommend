from DataService import Mongo
import pymongo
import time
import omdb

# Retrieving movie infomation from imdb for each movie in MovieLens database.
# db name: movieRecommend
# collection name: movie
# Adds fields to collection:
#       - year
#       - country
#       - language
#       - poster
#       - plot
#       - type
#       - runtime
#       - metascore
#       - rated
#       - imdb_rating
#       - imdb_votes
#       - genre
#       - director
#       - actors
#       - writer

def retrieve(mongo):

    progressInterval = 100    # How often should we print a progress report to the console?
    progressTotal = 34208      # Approximate number of total lines in the file.
    bulkSize = 100             # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    count = 0
    skipCount = 0

    print("[movieLensToIMDB] Starting Parse of movies.csv")
    startTime = time.time()

    # save all data in dict
    # output the data into MongoDB
    cursor = mongo.db["movie"].find({}, no_cursor_timeout=True)
    for cur_movie in cursor:
        count += 1
        if count % progressInterval == 0:
            print("[movieLensToIMDB] %5d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        cur_mid = cur_movie["mid"]
        cur_imdbid_len = len(str(cur_movie["imdbid"]))
        # Construct the real imdbid
        cur_imdbid = "tt"
        for i in range(7 - cur_imdbid_len):
            cur_imdbid += "0"
        cur_imdbid += str(cur_movie["imdbid"])

        # retrieve movie info from IMDB
        imdb_movie = omdb.imdbid(cur_imdbid)
        cur_genres = []
        for genre in imdb_movie["genre"].split(","):
            cur_genres.append(genre.strip())
        cur_actors = []
        for actor in imdb_movie["actors"].split(","):
            cur_actors.append(actor.strip())

        bulkPayload.find({"mid": cur_mid}).update({"$set": {
            "year": imdb_movie["year"], 
            "country": imdb_movie["country"], 
            "language": imdb_movie["language"], 
            "poster": imdb_movie["poster"], 
            "type": imdb_movie["type"], 
            "runtime": imdb_movie["runtime"], 
            "plot": imdb_movie["plot"], 
            "metascore": imdb_movie["metascore"], 
            "rated": imdb_movie["rated"], 
            "imdb_rating": imdb_movie["imdb_rating"], 
            "imdb_votes": imdb_movie["imdb_votes"], 
            "genres": cur_genres, 
            "director": imdb_movie["director"], 
            "actors": cur_actors, 
            "writer": imdb_movie["writer"]
            }})

        if count % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    if count % bulkSize > 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    print("[movieLensToIMDB] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensToIMDB] Found " + str(count) + " movies.")
    print("[movieLensToIMDB] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = Mongo("movieRecommend")
    retrieve(mongo)

if __name__ == "__main__":
    main()