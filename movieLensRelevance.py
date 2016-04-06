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
    bulkSize = 2000           # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkCount = 0
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
    count = 0
    skipCount = 0

    print("[movieLensRelevance] Starting Parse of tag_relevance.dat")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/tag_relevance.dat", "r", encoding = "utf8")

    # output the data into MongoDB
    for line in csv.reader(inCSV, delimiter = "\t"):
        count += 1
        if count % progressInterval == 0:
            print("[movieLensRelevance] %8d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        mid = int(line[0])
        tid = int(line[1])
        relevant_score = float(line[2])
        # consider only the relevant tag
        if relevant_score >= 0.5:
            cur_relevance = str(mid) + "," + str(relevant_score)
            bulkPayload.find({"tid": tid}).update({"$push": {"relevant_movie": cur_relevance}})
            bulkCount += 1
        if bulkCount >= bulkSize:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
            bulkCount = 0
    if bulkCount >= 0:
        try:
            bulkPayload.execute()
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