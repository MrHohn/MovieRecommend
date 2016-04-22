from DataService import Mongo
import pymongo
import time

# Parsing of MovieLens ratings.csv file.
# db name: movieRecommend
# collection name: user_rate
# Adds fields to collection:
#       -user id            (uid)
#       -user's ratings     (ratings)

def parse(mongo):
    progressInterval = 300000 # How often should we print a progress report to the console?
    progressTotal = 22884378  # Approximate number of total lines in the file.
    bulkSize = 2000           # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["user_rate"], ordered = False)
    count = 0
    userCount = 0
    skipCount = 0

    print("[movieLensRatings] Starting Parse of ratings.csv")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/ratings.csv", "r")
    # the first line is for attributes
    attrLine = inCSV.readline()

    # save all data in dict
    # output the data into MongoDB
    curDict = {} # {uid : ***, ratings : {[mid, rating], ...}}
    curList = []
    prevId = -1
    while 1:
        line = inCSV.readline()
        if not line:
            break

        count += 1
        if count % progressInterval == 0:
            print("[movieLensRatings] %8d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        curAttrs = line.split(",")
        curId = int(curAttrs[0])
        # store new user
        if prevId != -1 and curId != prevId:
            curDict["uid"] = prevId
            curDict["ratings"] = curList
            userCount += 1
            bulkPayload.insert(curDict)
            if userCount % bulkSize == 0:
                try:
                    bulkPayload.execute()
                except pymongo.errors.OperationFailure as e:
                    skipCount += len(e.details["writeErrors"])
                bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["user_rate"], ordered = False)
            curList = []
            curDict = {}
        # append new rating
        curList.append([int(curAttrs[1]), float(curAttrs[2])])
        prevId = curId
    # write out the last document
    curDict["uid"] = curId
    curDict["ratings"] = curList
    userCount += 1
    bulkPayload.insert(curDict)
    try:
        bulkPayload.execute()
    except pymongo.errors.OperationFailure as e:
        skipCount += len(e.details["writeErrors"])

    print("[movieLensRatings] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensRatings] Found " + str(userCount) + " users.")
    print("[movieLensRatings] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = Mongo("movieRecommend")
    parse(mongo)

if __name__ == "__main__":
    main()