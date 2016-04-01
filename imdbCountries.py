import imdbUtil
import time
import csv

# Parsing of IMDB countries.list file.
# Adds fields to movie database:
#		-Country of Origin (country)

def parse(mongo, collectionName):
	progressInterval = 100000 # How often should we print a progress report to the console?
	progressTotal = 1800000   # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of countries.list ===")
	startTime = time.time()
	with open("imdbdata/countries.list", encoding="latin1") as tsv:
		for line in csv.reader(tsv, delimiter="\t"):			
			title = -1
			country = -1
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
					title = value
				elif valueInd == 1: #Country
					country = value
				valueInd += 1

			# Update the corresponding movie entries in the database with the country info.
			if title != -1 and country != -1:
				bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(title)} ).update( {
						"$set": { "country":country }
					} )
				bulkCount += 1
				updateCount += 1

				if bulkCount >= bulkSize:
					bulkPayload.execute()
					bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
					bulkCount = 0

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with country information.")
