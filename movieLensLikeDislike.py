import pymongo
import time

# Creating of user like & dislike
# db name: movieLens
# collection name: user_rate
# Adds fields to collection:
#       -user like          ([mid, mid...])
#       -user dislike       ([mid, mid...])

def parse(mongo):
    progressInterval = 1000  # How often should we print a progress report to the console?
    progressTotal = 276961    # Approximate number of total lines in the file.
    bulkSize = 5000           # How many documents should we store in memory before inserting them into the database in bulk?
    bulkCount = 0
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["user_rate"], ordered = False)
    count = 0
    skipCount = 0

    print("[movieLensLikeDislike] Starting create of user like & dislike")
    startTime = time.time()

    # save all data in dict
    # output the data into MongoDB
    cursor = mongo.db["user_rate"].find({})
    for curUser in cursor:
        count += 1
        if count % progressInterval == 0:
            print("[movieLensLikeDislike] " + str(count) + " docs processed so far. (" + str(int(count * 100 / progressTotal)) + "%%) (%0.2fs)" % (time.time() - startTime))

        curLike = []
        curDislike = []
        curUid = curUser["uid"]
        curRatings = curUser["ratings"]
        for rating in curRatings:
            mid = rating[0]
            score = rating[1]
            if score >= 3.5:
                # print("user " + str(curUid) + " like movie " + str(mid))
                curLike.append(mid)
            if score <= 2.5:
                # print("user " + str(curUid) + " dislike movie " + str(mid))
                curDislike.append(mid)
        if len(curLike) > 0:
            bulkPayload.find( {"uid": curUid} ).update( {
                        "$set": { "like": curLike}
                    } )
            bulkCount += 1
        if len(curDislike) > 0:
            bulkPayload.find( {"uid": curUid} ).update( {
                        "$set": { "dislike": curDislike}
                    } )
            bulkCount += 1

        if bulkCount >= bulkSize:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["user_rate"], ordered = False)
            bulkCount = 0
    
    if bulkCount > 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])


    print("[movieLensLikeDislike] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensLikeDislike] Found " + str(count) + " user.")
    print("[movieLensLikeDislike] Skipped " + str(skipCount) + " insertions.")
