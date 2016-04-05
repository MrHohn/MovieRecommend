import DataService
import pymongo
import time

# Parsing of MovieLens Genome tags.dat file.
# db name: movieLens
# collection name: tag
# Adds fields to collection:
#       -tag id             (tid)
#       -tag content        (content)
#       -tag popularity     (popular)

def parse(mongo):
    progressInterval = 1000   # How often should we print a progress report to the console?
    progressTotal = 1128      # Approximate number of total lines in the file.
    bulkSize = 200            # How many documents should we store in memory before inserting them into the database in bulk?
    # List of documents that will be given to the database to be inserted to the collection in bulk.
    bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
    count = 0
    skipCount = 0

    print("[movieLensTags] Starting Parse of tags.dat")
    startTime = time.time()

    # open the ratings.csv and gather user's ratings into a list
    inCSV = open("movielensdata/tags.dat", "r")

    # save all data in dict
    # output the data into MongoDB
    while 1:
        line = inCSV.readline()
        if not line:
            break

        count += 1
        if count % progressInterval == 0:
            print("[movieLensTags] %4d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        curAttrs = line.split("\t")
        curDict = {} # {tid : ***, content: ***, popular: ***}
        curDict["tid"] = int(curAttrs[0])
        curDict["content"] = curAttrs[1]        
        curDict["popular"] = int(curAttrs[2])
        bulkPayload.insert(curDict)
        if count % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["tag"], ordered = False)
    try:
        bulkPayload.execute()
    except pymongo.errors.OperationFailure as e:
        skipCount += len(e.details["writeErrors"])


    print("[movieLensTags] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[movieLensTags] Found " + str(count) + " tags.")
    print("[movieLensTags] Skipped " + str(skipCount) + " insertions.")


def main():
    mongo = DataService.Mongo("movieLens")
    parse(mongo)

if __name__ == "__main__":
    main()