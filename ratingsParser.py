import json
import DataService

class RatingsParser(object):

    @classmethod
    def parse_ratings(self):
        # open the ratings.csv and gather user's ratings into a list
        inCSV = open("ml-latest-small/ratings.csv", "r")
        # the first line is for attributes
        attrLine = inCSV.readline()
        # record the needed attributes
        print("[main] Parsed attributes")
        attrs = attrLine.split(",")
        attrLen = len(attrs)
        for attr in attrs:
            print("- " + attr)

        # save all data in dict
        # output the data into MongoDB (or to JSON file - commented out)
        print("[main] Start converting and storing user ratings...")
        mongo = DataService.Mongo("movieRecommend")
        # out = open('user_rating.json', 'w')
        curDict = {} # {uid : ***, ratings : {[mid, rating], ...}}
        curList = []
        prevId = -1
        while 1:
            line = inCSV.readline()
            if not line:
                break

            curAttrs = line.split(",")
            curId = int(curAttrs[0])
            # store new user
            if prevId != -1 and curId != prevId:
                curDict["uid"] = prevId
                curDict["ratings"] = curList
                mongo.insert_one("user_rate", curDict);
                # json_data = json.dumps(curDict, sort_keys = True)
                # out.write(json_data + "\n");
                curList = []
                curDict = {}
            # append new rating
            curList.append([int(curAttrs[1]), float(curAttrs[2])])
            prevId = curId
        # write out the last object
        curDict["uid"] = curId
        curDict["ratings"] = curList
        mongo.insert_one("user_rate", curDict);
        # json_data = json.dumps(curDict, sort_keys = True)
        # out.write(json_data + "\n");
        print("[main] Done.")


    @classmethod
    def build_movieToUser_index(self):
        # open the ratings.csv and build movie to user ratings index
        inCSV = open("ml-latest-small/ratings.csv", "r")
        # the first line is for attributes
        attrLine = inCSV.readline()
        # output the data into MongoDB
        print("[main] Start building index...")
        mongo = DataService.Mongo("movieRecommend")
        curProgress = 0
        flag = 5000
        while 1:
            line = inCSV.readline()
            if not line:
                break

            curAttrs = line.split(",")
            curUser = int(curAttrs[0])
            curMovie = int(curAttrs[1])
            # check if there is a record in database
            curDict = mongo.find_one("movieToUser", {"mid": curMovie})
            newMovie = False
            if curDict is None:
                newMovie = True
                curDict = {}
                curDict["mid"] = curMovie
                curDict["userList"] = []
            curDict["userList"].append(curUser)
            # if found new movie, insert it
            if newMovie:
                mongo.insert_one("movieToUser", curDict)
            else:
                mongo.replace_one("movieToUser", {"mid": curMovie}, curDict)
            # progress indicating
            curProgress = curProgress + 1
            if curProgress > flag:
                print(flag)
                flag += 5000

        print("[main] Done.")


def main():
    # RatingsParser.parse_ratings()
    RatingsParser.build_movieToUser_index()

if __name__ == "__main__":
    main()