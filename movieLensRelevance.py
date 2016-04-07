import DataService
import pymongo
import time
import csv

# Parsing of MovieLens Genome tag_relevance.dat file.
# db name: movieLens
# collection name: tag
# Adds fields to collection:
#       -"movies id,relevant_score"  (relevant_movie)

def parse(mongo):
    progressInterval = 200000 # How often should we print a progress report to the console?
    progressTotal = 10979952  # Approximate number of total lines in the file.
    bulkSize_tag = 2000       # How many documents should we store in memory before inserting them into the database in bulk?
    bulkCount_tag = 0
    bulkPayload_tag = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
    bulkSize_movie = 300      # How many documents should we store in memory before inserting them into the database in bulk?
    bulkPayload_movie = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
    bulkCount_movie = 0
    tag_num = 1128
    count = 0
    skipCount = 0

    print("[movieLensRelevance] Starting Parse of tag_relevance.dat")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/tag_relevance.dat", "r", encoding = "utf8")

    # output the data into MongoDB
    tags_relevance = []
    for line in csv.reader(inCSV, delimiter = "\t"):
        count += 1
        if count % progressInterval == 0:
            print("[movieLensRelevance] %8d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        mid = int(line[0])
        tid = int(line[1])
        relevant_score = float(line[2])
        # consider only the relevant tag
        if relevant_score >= 0.5:
            # append the new movie to tag
            cur_movie_relevance = str(mid) + "," + str(relevant_score)
            bulkPayload_tag.find({"tid": tid}).update({"$push": {"relevant_movie": cur_movie_relevance}})
            bulkCount_tag += 1
            # append the new tag to movie
            cur_tag_relevance = str(tid) + "," + str(relevant_score)
            tags_relevance.append(cur_tag_relevance)

        if count % tag_num == 0:
            bulkPayload_movie.find({"mid": mid} ).update({"$set": {"tags": tags_relevance}})
            bulkCount_movie += 1
            tags_relevance = []

        if bulkCount_tag >= bulkSize_tag:
            try:
                bulkPayload_tag.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload_tag = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
            bulkCount_tag = 0

        if bulkCount_movie >= bulkSize_movie:
            try:
                bulkPayload_movie.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload_movie = pymongo.bulk.BulkOperationBuilder(mongo.db["movie"], ordered = False)
            bulkCount_movie = 0

    if bulkCount_tag >= 0:
        try:
            bulkPayload_tag.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    if bulkCount_movie >= 0:
        try:
            bulkPayload_movie.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    print("[movieLensRelevance] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensRelevance] Found " + str(count) + " relevances.")
    print("[movieLensRelevance] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = DataService.Mongo("movieLens")
    parse(mongo)

if __name__ == "__main__":
    main()