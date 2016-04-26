from DataService import Mongo
import pymongo
import time

# build people to movies index from imdb database
# runtime: 3-5 minutes

def build(mongo):

    progressInterval = 1000
    progressTotal = 1251126
    count = 0

    print("[imdbPeopleIndex] Starting build people to movies index from imdb database...")
    startTime = time.time()

    peoples_dict = {}
    cursor = mongo.db["movies"].find({}, no_cursor_timeout=True)
    for cur_movie in cursor:
        count += 1
        if count % progressInterval == 0:
            print("[imdbPeopleIndex] %7d movies processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))

        cur_title_imdb = cur_movie["imdbtitle"]
        if "crew" in cur_movie:
            for people in cur_movie["crew"]:
                if people not in peoples_dict.keys():
                    peoples_dict[people] = [cur_title_imdb]
                else:
                    peoples_dict[people].append(cur_title_imdb)
        if "cast" in cur_movie:
            for people in cur_movie["cast"]:
                if people not in peoples_dict.keys():
                    peoples_dict[people] = [cur_title_imdb]
                else:
                    peoples_dict[people].append(cur_title_imdb)

    # store people to movies index into integration database
    db_integration = mongo.client["integration"]
    bulkSize = 2000
    bulkPayload = pymongo.bulk.BulkOperationBuilder(db_integration["peoples"], ordered = False)
    skipCount = 0
    bulkCount = 0
    progressInterval = 5000
    progressTotal = len(peoples_dict)

    for people in peoples_dict.keys():
        bulkCount += 1
        if bulkCount % progressInterval == 0:
            print("[imdbPeopleIndex] %7d peoples processed so far. (%d%%) (%0.2fs)" % (bulkCount, int(bulkCount * 100 / progressTotal), time.time() - startTime))

        bulkPayload.insert({
            "people": people,
            "popularity": len(peoples_dict[people]),
            "movies" : peoples_dict[people]
            })

        if bulkCount % bulkSize == 0:
            try:
                bulkPayload.execute()
            except pymongo.errors.OperationFailure as e:
                skipCount += len(e.details["writeErrors"])
            bulkPayload = pymongo.bulk.BulkOperationBuilder(db_integration["peoples"], ordered = False)
    if bulkCount % bulkSize > 0:
        try:
            bulkPayload.execute()
        except pymongo.errors.OperationFailure as e:
            skipCount += len(e.details["writeErrors"])

    print("[imdbPeopleIndex] Build Complete (%0.2fs)" % (time.time() - startTime))
    print("[imdbPeopleIndex] Found " + str(count) + " movies.")
    print("[imdbPeopleIndex] Found " + str(len(peoples_dict)) + " peoples.")
    print("[imdbPeopleIndex] Skipped " + str(skipCount) + " insertions.")
    db_integration["peoples"].create_index([("people", pymongo.ASCENDING)])
    print("[imdbPeopleIndex] Created index for people in peoples from integration database")


def main():
    mongo = Mongo("imdb")
    build(mongo)

if __name__ == "__main__":
    main()