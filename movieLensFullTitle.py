from DataService import Mongo
import pymongo
import time

# find the full imdb title from imdb database
# with this approach, 872 (out of 34208) movies still failed to be assigned the full imdb title
# mainly due to french or other languages

def retrieve(mongo):

    progressInterval = 500     # How often should we print a progress report to the console?
    progressTotal = 34208      # Approximate number of total lines in the file.
    bulkSize = 500             # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    count = 0
    skipCount = 0
    unfound = 0

    db_imdb = mongo.client["imdb"]
    db_movieRecommend = mongo.client["movieRecommend"]

    print("[movieLensFullTitle] Starting retrieve of movie info from imdb database...")
    startTime = time.time()

    cursor = db_movieRecommend["movie"].find({}, no_cursor_timeout=True)
    for cur_movie in cursor:
        count += 1
        if count % progressInterval == 0:
            print("[movieLensFullTitle] %5d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        cur_mid = cur_movie["mid"]
        cur_title_imdb = cur_movie["title_imdb"]
        notFound = True
        # handle the duplicated case
        imdb_movie_cursor = db_imdb["movies"].find({"title": cur_title_imdb})
        for imdb_movie in imdb_movie_cursor:
            if imdb_movie["year"] == cur_movie["year"]:
                bulkPayload.find({"mid": cur_mid}).update({"$set": {
                    "title_full": imdb_movie["imdbtitle"]
                    }})
                notFound = False
                break

        if notFound:
            unfound += 1

        if count % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(db_movieRecommend["movie"], ordered = False)

    if count % bulkSize > 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    print("[movieLensFullTitle] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensFullTitle] Found " + str(count) + " movies.")
    print("[movieLensFullTitle] Unfound " + str(unfound) + " movies.")
    print("[movieLensFullTitle] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = Mongo("movieRecommend")
    db_imdb = mongo.client["imdb"]
    db_imdb["movies"].create_index([("title", pymongo.ASCENDING)])
    print("[movieLensFullTitle] Created index for title in movies")
    retrieve(mongo)
    db_movieRecommend = mongo.client["movieRecommend"]
    db_movieRecommend["movie"].create_index([("title_full", pymongo.ASCENDING)])
    print("[movieLensFullTitle] Created index for title_full in movie")

if __name__ == "__main__":
    main()