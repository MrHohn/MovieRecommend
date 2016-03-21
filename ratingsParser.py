import json
import DataService

def main():
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
	print("[main] Start converting and storing data ...")
	mongo = DataService.Mongo("movieRecommend")
	# out = open('user_rating.json', 'w')
	curDict = {} # {uid : ***, ratings : {[mid, rating], ...}}
	curList = []
	prevId = -1
	while 1:
		line = inCSV.readline()
		if not line:
			print("[main] Done.")
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


if __name__ == "__main__":
	main()