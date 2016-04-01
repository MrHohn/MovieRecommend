import imdbUtil
import time
import re

# Parsing of IMDB aka-titles.list file.
# Appends to following movie database field:
#		-All Titles (title)

def parse(mongo, collectionName):
	progressInterval = 100000  # How often should we print a progress report to the console?
	progressTotal = 1000000     # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of aka-titles.list ===")
	startTime = time.time()
	f = open("imdbdata/aka-titles.list", encoding="latin1")
	title = -1
	isEpisode = False
	akatitles = []

	for line in f:			
		count += 1
		if count % progressInterval == 0:
			print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

		if "(aka" in line:
			if not isEpisode:
				akatitles.append(imdbUtil.simpleTitle(line[line.index("(aka")+5:]))
		else:
			if imdbUtil.isEpisode(line):
				isEpisode = True
			else:
				# Update the corresponding movie entry in the database with the alternate title info.
				if len(akatitles) > 0:
					bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(title)} ).update( {
						"$addToSet": { "title": {"$each" : akatitles} }
					} )
					bulkCount += 1
					updateCount += 1

				isEpisode = False
				title = line
				akatitles = []		

		if bulkCount >= bulkSize:
			bulkPayload.execute()
			bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
			bulkCount = 0
	f.close()

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with alternate title information.")