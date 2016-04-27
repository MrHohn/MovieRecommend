import imdbUtil
import time
import csv
import pymongo
import imdbMovieLensTags

# Parsing of IMDB keywords.list file.
# Adds fields to movie database:
#		-List of keywords (keywords)

def parse(mongo, collectionName):
	progressInterval = 250000 # How often should we print a progress report to the console?
	progressTotal = 6400000   # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0
	removeCount = 0
	ignoreUntil = "8: THE KEYWORDS LIST" #Ignore parsing of lines until one matching this string is found
	ignoring = True

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Starting Parse of keywords.list ===")
	startTime = time.time()
	with open("imdbdata/keywords.list", encoding="latin1") as tsv:
		keywords = []
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
					if imdbUtil.isEpisode(value):
						break
					lastTitle = title
					title = value

					# We moved onto the next movie; update the the database with the keyword info of the previous movie first.
					if title != lastTitle and len(keywords) > 0:
						bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(lastTitle)} ).update( {
							"$set": { "keywords":keywords.copy() }
						} )
						bulkCount += 1
						keywords = []
				elif valueInd == 1: #Keyword
					keywords.append(value)
					updateCount += 1
				valueInd += 1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
				bulkCount = 0

	if len(keywords) > 0:
		bulkPayload.find( {"imdbtitle":imdbUtil.formatTitle(title)} ).update( {
				"$set": { "keywords":keywords.copy() }
		} )
		bulkCount += 1

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Added", str(updateCount), "keywords to movies in the database.")

#Parses all keywords and their respective frequencies, and stores this data into the "keywords" collection.
def collectKeywords(mongo):
	ignoreUntil = "4: The Keywords"   #Ignore parsing of lines until one matching this string is found
	stopAfter = "5: Submission Rules" #Stop parsing the file once this line is read
	ignoring = True
	finished = False
	keywordCount = 0

	bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["keywords"], ordered=False)	
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	print("=== Collecting List and Frequency of all Keywords ===")
	startTime = time.time()
	with open("imdbdata/keywords.list", encoding="latin1") as tsv:
		for line in csv.reader(tsv, delimiter="\t"):	
			for value in line:
				if value == "":
					continue
				if value == ignoreUntil:
					ignoring = False
					break
				if value == stopAfter:
					finished = True
					break
				if ignoring:
					break
				
				if "(" in value:
					keyword = value[:value.rfind("(")-1].strip()
					frequency = int(value[value.rfind("(")+1:value.rfind(")")])
					bulkPayload.insert({"keyword":keyword, "count":frequency})
					keywordCount += 1
					bulkCount += 1

			if bulkCount >= bulkSize:
				bulkPayload.execute()
				bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["keywords"], ordered=False)	
				bulkCount = 0

			if finished:
				break

	if bulkCount >= 0:
		bulkPayload.execute()

	print("[*] Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Added", str(keywordCount), "items to the keywords collection.")

def processMovieLensLinks(mongo):
	newCounts = {}
	newKeywords = set()
	bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["movies"], ordered=False)

	print("=== Starting MovieLens tag integration ===")
	startTime = time.time()
	# Update movie keywords lists based on data in imdbMovieLensTags.py
	for keyFilter in imdbMovieLensTags.imdbKeywords:
		if keyFilter[0] == "*":
			# Wildcard search
			findFilt = {"keywords":{"$regex":sanitizeRegex(keyFilter[1:])}}

			if keyFilter in imdbMovieLensTags.imdbIgnore:
				ignoreFilters = imdbMovieLensTags.imdbIgnore[keyFilter]
				andfilt = [findFilt.copy()]
				for ignore in ignoreFilters:
					if ignore[0] == "*":
						andfilt.append({"keywords":{"$not":{"$regex":sanitizeRegex(ignore[1:])}}})
					else:
						andfilt.append({"keywords":{"$not":ignore}})
				findfilt = {"$and":andfilt}

			bulkPayload.find( findFilt ).update( {
							"$addToSet": { "keywords": { "$each":imdbMovieLensTags.imdbKeywords[keyFilter] } }
						} )

		elif keyFilter[0] == ":":
			# Mongo search
			bulkPayload.find( imdbMovieLensTags.getMongoSearch(keyFilter) ).update( {
							"$addToSet": { "keywords": { "$each":imdbMovieLensTags.imdbKeywords[keyFilter] } }
						} )

		else:
			# Simple search
			bulkPayload.find( {"keywords":keyFilter} ).update( {
							"$addToSet": { "keywords": { "$each":imdbMovieLensTags.imdbKeywords[keyFilter] } }
						} )

		for keyword in imdbMovieLensTags.imdbKeywords[keyFilter]:
			newKeywords.add(keyword)

	bulkPayload.execute()
	print("[*] Complete (%0.2fs)" % (time.time()-startTime))

	# Update keywords and counts in the keywords database
	print("=== Updating keyword counts ===")
	startTime = time.time()

	bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["keywords"], ordered=False)
	for keyword in newKeywords:
		bulkPayload.insert({"keyword":keyword, "count":0})
	bulkPayload.execute()

	bulkPayload = pymongo.bulk.BulkOperationBuilder(mongo.db["keywords"], ordered=False)
	allKeywords = mongo.db["keywords"].find({})
	for entry in allKeywords:
		keyword = entry["keyword"]
		bulkPayload.find({"keyword":keyword}).update({"$set":{"count":mongo.db["movies"].find( {"keywords":keyword} ).count()}})
	bulkPayload.execute()
	print("[*] Complete (%0.2fs)" % (time.time()-startTime))

def sanitizeRegex(regex):
	return regex.replace("(","\(").replace(")","\)")