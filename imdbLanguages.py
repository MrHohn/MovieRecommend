import imdbUtil
import time
import csv

# Parsing of IMDB language.list file.
# Adds fields to movie database:
#		-Language (languages)

def parse(mongo, collectionName):
	progressInterval = 100000  # How often should we print a progress report to the console?
	progressTotal = 1800000     # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of language.list ===")
	startTime = time.time()
	with open("imdbdata/language.list", encoding="latin1") as tsv:
		languages = []
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
				if valueInd == 0: #Movie Title
					if imdbUtil.isEpisode(value):
						break
					lastTitle = title
					title = value

					# We moved onto the next movie; update the the database with the language info of the previous movie first.
					if title != lastTitle and len(languages) > 0:
						bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(lastTitle)} ).update( {
							"$set": { "languages":languages.copy() }
						} )
						updateCount += 1
						bulkCount += 1
						languages = []
				elif valueInd == 1: #Company
					languages.append(__formatLanguage(value))
				valueInd += 1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
				bulkCount = 0

	if len(languages) > 0:
		bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(lastTitle)} ).update( {
			"$set": { "languages":languages.copy() }
		} )
		updateCount += 1
		bulkCount += 1

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with language information.")

def __formatLanguage(language):
	if "(" in language:
		return language[:language.index("(")-1]
	else:
		return language