from DataService import Mongo
import pymongo
import time
import csv

# Parsing of ANEW all.csv file.
# db name: movieRecommend
# collection name: anew
# Adds fields to collection:
#       -ANEW "all" list  (with "type" = "all")

def parse(mongo):
    progressInterval = 1000   # How often should we print a progress report to the console?
    progressTotal = 1031      # Approximate number of total lines in the file.
    count = 0

    print("[anewAll] Starting Parse of all.csv")
    startTime = time.time()

    # open the all.csv and gather all content into a dictionary
    inCSV = open("ANEW/all.csv", "r", encoding = "utf8")

    # output the data into MongoDB
    anewAllList = {}
    for line in csv.reader(inCSV, delimiter = ","):
        count += 1
        if count % progressInterval == 0:
            print("[anewAll] %4d lines processed so far. (%d%%) (%0.2fs)" % (count, int(count * 100 / progressTotal), time.time() - startTime))
        # skip the first line
        if count == 1:
        	continue

        cur_word = line[0]
        cur_dict = {}
        cur_dict["id"] = int(line[1])
        cur_dict["valence_mean"] = float(line[2])
        cur_dict["valence_sd"] = float(line[3])
        cur_dict["arousal_mean"] = float(line[4])
        cur_dict["arousal_sd"] = float(line[5])
        cur_frequency = line[8]
        if cur_frequency == ".":
        	cur_dict["frequency"] = 0
        else:
        	cur_dict["frequency"] = int(cur_frequency)
        anewAllList[cur_word] = cur_dict

    doc = {}
    doc["type"] = "all"
    doc["dict"] = anewAllList
    mongo.db["anew"].insert_one(doc)
    print("[anewAll] Parse Complete (%0.2fs)" % (time.time() - startTime))
    print("[anewAll] Found " + str(count) + " words.")


def main():
    # Add ANEW all list into database.
    # runtime: (0.05s)
    mongo = Mongo("movieRecommend")
    parse(mongo)

if __name__ == "__main__":
    main()