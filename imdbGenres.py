import imdbUtil
import time
import csv

# Parsing of IMDB genres.list file.
# Adds fields to movie database:
#		-Genre (genres)
#
# Also excludes all movies from the database tagged under the genre "Adult", because I don't
# want to take the chance of one of those popping up during a live demo.....

def parse(mongo, collectionName):
	progressInterval = 100000 # How often should we print a progress report to the console?
	progressTotal = 2200000   # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0
	removeCount = 0
	ignoreUntil = "8: THE GENRES LIST" #Ignore parsing of lines until one matching this string is found
	ignoring = True

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of genres.list ===")
	startTime = time.time()
	with open("imdbdata/genres.list", encoding="latin1") as tsv:
		genres = []
		title = -1
		lastTitle = -1
		for line in csv.reader(tsv, delimiter="\t"):			
			valueInd = 0 #Which column in the TSV file are we reading?
			count += 1
			if count % progressInterval == 0:
				print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

			for value in line:
				if value == "":
					continue
				if value == ignoreUntil:
					ignoring = False
				if ignoring:
					break
				if valueInd == 0: #Movie Title
					if imdbUtil.isShow(value):
						break
					lastTitle = title
					title = value

					# We moved onto the next movie; update the the database with the genre info of the previous movie first.
					if title != lastTitle and len(genres) > 0:
						if "Adult" in genres:
							removeCount += 1
							bulkPayload.find( {"title":imdbUtil.formatTitle(lastTitle)} ).remove()
						else:
							updateCount += 1
							bulkPayload.find( {"title":imdbUtil.formatTitle(lastTitle)} ).update( {
								"$set": { "genres":genres.copy() }
							} )
						bulkCount += 1
						genres = []
				elif valueInd == 1: #Genre
					genres.append(value)
				valueInd += 1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
				bulkCount = 0

	if len(genres) > 0:
		if "Adult" in genres:
			removeCount += 1
			bulkPayload.remove( {"title":imdbUtil.formatTitle(title)} )
		else:
			updateCount += 1
			bulkPayload.find( {"title":imdbUtil.formatTitle(title)} ).update( {
				"$set": { "genres":genres.copy() }
			} )
		bulkCount += 1

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with genre information.")
	print("[*] Removed", str(removeCount), "pornographic films from the database.")
