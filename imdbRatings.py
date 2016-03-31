import imdbUtil
import time
import re

# Parsing of IMDB mpaa-ratings-reasons.list file.
# Adds fields to movie database:
#		-Age Rating (rating)

def parse(mongo, collectionName):
	progressInterval = 50000  # How often should we print a progress report to the console?
	progressTotal = 71000     # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of mpaa-ratings-reasons.list ===")
	startTime = time.time()
	f = open("imdbdata/mpaa-ratings-reasons.list", encoding="latin1")
	title = -1
	rating = -1

	for line in f:			
		count += 1
		if count % progressInterval == 0:
			print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

		if line[:3] == "MV:":
			title = line[4:]
		if line[:3] == "RE:" and "Rated" in line:
			rating = __extractRating(line)

		# Update the corresponding movie entries in the database with the country info.
		if title != -1 and rating != -1:
			bulkPayload.find( {"title":imdbUtil.formatTitle(title)} ).update( {
					"$set": { "rating":rating }
				} )
			bulkCount += 1
			updateCount += 1
			title = -1
			rating = -1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
				bulkCount = 0
	f.close()

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with age rating information.")

def __extractRating(string):
	ratingRegex = re.findall(r"Rated\s[A-Z0-9\-]+", string)
	if len(ratingRegex) > 0:
		ratingStr = ratingRegex[0]
		return ratingStr[ratingStr.index(" ")+1:]
	return ""