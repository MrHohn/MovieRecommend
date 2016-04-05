import pymongo
import time
import csv

# Parsing of MovieLens movies.csv file.
# db name: movieLens
# collection name: movie
# Adds fields to collection:
#       -movie id           (mid)
#       -movie title        (title)

def parse(mongo):
    progressInterval = 10000  # How often should we print a progress report to the console?
    progressTotal = 34209     # Approximate number of total lines in the file.
    bulkSize = 2000           # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    count = 0
    skipCount = 0
    bulkCount = 0

    print("[movieLensMovies] Starting Parse of movies.csv")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/movies.csv", "r", encoding = "utf8")
    # the first line is for attributes
    attrLine = inCSV.readline()

    # save all data in dict
    # output the data into MongoDB
    for line in csv.reader(inCSV, delimiter = ","):
        count += 1
        # skip the first line
        if count == 1:
            continue
        if count % progressInterval == 0:
            print("[movieLensMovies] " + str(count) + " lines processed so far. (" + str(int(count * 100 / progressTotal)) + "%%) (%0.2fs)" % (time.time() - startTime))

        mid = int(line[0])
        title = line[1]
        bulkPayload.find( {"mid": mid} ).update({"$set": { "title": title}})
        bulkCount += 1
        if bulkCount >= bulkSize:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
            bulkCount = 0
    if bulkCount >= 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    print("[movieLensMovies] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensMovies] Found " + str(count) + " movies.")
    print("[movieLensMovies] Skipped " + str(skipCount) + " insertions.")

