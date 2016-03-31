import pymongo
import imdbUtil
import time
import csv

# Parsing of IMDB movies.list file.
# Adds fields to database:
#		-Movie Title 	(title)
#		-Release Year 	(year)
#		-Is TV Show?	(tv)

def parse(mongo, collectionName):
	progressInterval = 100000 # How often should we print a progress report to the console?
	progressTotal = 3700000   # Approximate number of total lines in the file.
	bulkSize = 5000 		  # How many documents should we store in memory before inserting them into the database in bulk?
	bulkCount = 0
	pendingDoc = {}			  # Current document we are parsing data for. Once finished, will be appended to bulkPayload.
	# List of documents that will be given to the database to be inserted to the collection in bulk.
	bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db[collectionName], ordered=False)		  
	count = 0
	movieCount = 0
	tvCount = 0
	skipCount = 0

	print("=== Starting Parse of movies.list ===")
	startTime = time.time()
	with open("imdbdata/movies.list", encoding="latin1") as tsv:
		for line in csv.reader(tsv, delimiter="\t"):			
			title = -1
			year = -1
			valueInd = 0 #Which column in the TSV file are we reading?
			isEpisode = False
			count += 1
			if count % progressInterval == 0:
				print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

			# Parse the text data from the read TSV line
			for value in line:
				if value == "":
					continue
				if valueInd == 0: #Movie Title
					if imdbUtil.isEpisode(value):
						isEpisode = True
						break
					title = value
				elif valueInd == 1: #Year
					year = imdbUtil.parseYear(value)
				valueInd += 1

			# This line cooresponds to a movie. Add it to the database.
			if title != -1 and year != -1:
				if "title" in pendingDoc and imdbUtil.stripEpisode(title) != imdbUtil.stripEpisode(pendingDoc["title"]):
					if "tv" in pendingDoc:
						tvCount += 1
					else:
						movieCount += 1
					pendingDoc["title"] = imdbUtil.formatTitle(pendingDoc["title"])
					bulkPayload.insert(pendingDoc.copy())
					bulkCount += 1
					pendingDoc.clear()

				if bulkCount >= bulkSize:
					try:
						bulkPayload.execute()
					except pymongo.errors.OperationFailure as e:
						skipCount += len(e.details["writeErrors"])
					bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db[collectionName], ordered=False)	
					bulkCount = 0

				pendingDoc["title"] = title
				pendingDoc["year"] = year

			#This line cooresponds to a TV episode. Mark the previously logged movie as actually being a TV show, rather than a movie.
			if isEpisode:
				pendingDoc["tv"] = 1

	if pendingDoc != {}:
		if "tv" in pendingDoc:
			tvCount += 1
		else:
			movieCount += 1
		pendingDoc["title"] = imdbUtil.formatTitle(pendingDoc["title"])
		bulkPayload.insert(pendingDoc.copy())
		bulkCount += 1

	if bulkCount > 0:
		try:
			bulkPayload.execute()
		except pymongo.errors.OperationFailure as e:
			skipCount += len(e.details["writeErrors"])

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Found", str(movieCount), "movies.")
	print("[*] Found", str(tvCount), "TV shows.")
	print("[*] Skipped", str(skipCount), "insertions.")
