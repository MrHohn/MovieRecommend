import imdbUtil
import time

# Parsing of IMDB movie-links.list file.
# Adds fields to movie database:
#		-Related Films (related)

def parse(mongo, collectionName):
	progressInterval = 100000 # How often should we print a progress report to the console?
	progressTotal = 2700000   # Approximate number of total lines in the file.	  
	count = 0
	updateCount = 0

	bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
	bulkSize = 5000 		  # How many queries should we store in memory before sending them to the database in bulk?
	bulkCount = 0

	#Relations we are interested in logging
	relations = ["follows", "followed by", "remake of", "remade as", "version of", "spin off from"]

	print("=== Starting Parse of movie-links.list ===")
	startTime = time.time()
	f = open("imdbdata/movie-links.list", encoding="latin1")
	title = -1
	links = []
	nextReady = True #are we finished with the current movie's information and ready to move onto the next?

	for line in f:			
		count += 1
		if count % progressInterval == 0:
			print(str(count), "lines processed so far. ("+str(int((count/progressTotal)*100))+"%%) (%0.2fs)" % (time.time()-startTime))

		if nextReady:
			#Title Line
			title = imdbUtil.formatTitle(line)
			nextReady = False
		if line.strip() == "":
			# Update the corresponding movie entries in the database with the relational info.
			if len(links) > 0 and not imdbUtil.isEpisode(title):
				bulkPayload.find( {"imdbtitle":title} ).update( {
						"$set": { "related":links }
					} )				
				bulkCount += 1
				updateCount += 1
			links = []
			nextReady = True

		for relation in relations:
			if relation in line:
				#Relation Line
				links.append(imdbUtil.formatTitle(line[line.index(relation)+len(relation)+1:].strip()[:-1]))
				break

		if bulkCount >= bulkSize:
			bulkPayload.execute()
			bulkPayload = mongo.db[collectionName].initialize_unordered_bulk_op()
			bulkCount = 0
	f.close()

	if bulkCount > 0:
		bulkPayload.execute()

	print("[*] Parse Complete (%0.2fs)" % (time.time()-startTime))
	print("[*] Attemped updating", str(updateCount), "movies with taxonomic information.")