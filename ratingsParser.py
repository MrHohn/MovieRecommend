import json

def main():
	# open the ratings.csv and gather user's ratings into a list
	inCSV = open("ml-latest-small/ratings.csv", "r")
	# the first line is for attributes
	attrLine = inCSV.readline()
	print("--- Content of the first line ---")
	print(attrLine)

	# record the needed attributes
	print("--- Parsed attributes ---")
	attrs = attrLine.split(",")
	attrLen = len(attrs)
	for attr in attrs:
		print("- " + attr)

	# save all data in dict
	# output the data to JSON file and MongoDB both
	print("Start converting to JSON...")
	out = open('ratings.json', 'w')
	curDict = {} # {uid : ***, ratings : {[mid, rating], ...}}
	curList = []
	prevId = "nobody"
	while 1:
		line = inCSV.readline()
		if not line:
			print("Done.")
			break

		curAttrs = line.split(",")
		curId = curAttrs[0]
		# store new user
		if prevId != "nobody" and curId != prevId:
			curDict["userId"] = curId
			curDict["ratings"] = curList
			json_data = json.dumps(curDict, sort_keys = True)
			out.write(json_data + "\n");
			curList = []
			curDict = {}
		# append new rating
		curList.append([curAttrs[1], curAttrs[2]])
		prevId = curId
	# write out the last object
	curDict["userId"] = curId
	curDict["ratings"] = curList
	json_data = json.dumps(curDict, sort_keys = True)
	out.write(json_data + "\n");


if __name__ == "__main__":
	main()