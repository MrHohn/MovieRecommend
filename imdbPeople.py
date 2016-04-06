import imdbUtil
import time
import csv

# Parsing of IMDB lists pertaining to people.
# 		field: The field in the database to add entries to (ie: cast, crew)
#		listfile: The IMDB list to parse (actors.list, directors.list, etc)
#		progressTotal: approximate number of lines in the file (for progress reporting)

def parse(mongo, collectionName, field, listfile, progressTotal):
	progressInterval = 250000 # How often should we print a progress report to the console?  
	if progressTotal > 10000000:
		progressInterval = 500000
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	#Conditions that must be found in the file before names will start being parsed
	startFlag = False
	startFlag2 = False
	endFlag = False

	print("=== Starting Parse of "+listfile+" ===")
	startTime = time.time()
	with open("imdbdata/"+listfile, encoding="latin1") as tsv:
		name = -1
		for line in csv.reader(tsv, delimiter="\t"):			
			foundOnLine = False #Did we find any content on this line? Lines with no content reset and set-up for the next name.
			count += 1
			if count % progressInterval == 0:
				print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

			for value in line:
				if endFlag:
					break
				if value.strip()[:4] == "Name":
					startFlag = True
					break
				if value.strip()[:4] == "----" and startFlag:
					startFlag2 = True
					break
				if not startFlag or not startFlag2:
					break
				if "-----------------" in value and startFlag and startFlag2:
					endFlag = True
					break

				if value == "":
					continue
				else:
					foundOnLine = True
					if name == -1:
						name = imdbUtil.formatName(value)
						updateCount += 1

					# Skipping logging people for TV episodes, because there's no easy way to tell if a person is a "main character", and I don't want to be logging
					# every single minor/cameo person who appeared in some single episode of some series. Plus, we're more concered about movies, not TV shows.
					elif not imdbUtil.isEpisode(value): 
						bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(value)} ).update( {
							"$addToSet": { field: name }
						} )
						bulkCount += 1

			if not foundOnLine:
				name = -1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
				bulkCount = 0

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating movies with information about", str(updateCount), "people.")
