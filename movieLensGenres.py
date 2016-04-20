from DataService import Mongo
import pymongo
import time
import csv

# Parsing of MovieLens movies.csv file.
# db name: movieRecommend
# collection name: movie
# Adds fields to collection:
#       -movie genres        (genres)

def parse(mongo):
    progressInterval = 5000   # How often should we print a progress report to the console?
    progressTotal = 34209      # Approximate number of total lines in the file.
    bulkSize = 2000            # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    count = 0
    skipCount = 0
    no_genres_count = 0

    print("[movieLensMoviesGenres] Starting Parse of movies.csv")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/movies.csv", "r", encoding = "utf8")

    # save all data in dict
    # output the data into MongoDB
    for line in csv.reader(inCSV, delimiter = ","):
        count += 1
        if count == 1:
            continue
        if count % progressInterval == 0:
            print("[movieLensMoviesGenres] %5d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        mid = int(line[0])
        genres_string = line[2]
        if genres_string == "(no genres listed)":
            no_genres_count += 1
            continue
        genres = []
        for genre in genres_string.split("|"):
            genres.append(genre)

        bulkPayload.find( {"mid": mid} ).update({"$set": {"genres": genres}})
        if count % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    try:
        bulkPayload.execute()
    except pymongo.errors.OperationFailure as e:
        skipCount += len(e.details["writeErrors"])

    print("[movieLensMoviesGenres] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensMoviesGenres] Found " + str(count - 1) + " movies.")
    print("[movieLensMoviesGenres] Unclassified count: " + str(no_genres_count))
    print("[movieLensMoviesGenres] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = Mongo("movieRecommend")
    parse(mongo)

if __name__ == "__main__":
    main()