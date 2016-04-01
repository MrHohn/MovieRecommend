import DataService
import imdbMovies
import imdbAkaTitles
import imdbCountries
import imdbRatings
import imdbGenres
import imdbCompanies
import imdbLanguages
import imdbRelatedFilms
import imdbKeywords

# Approximate total run time: 15 minutes

collectionName = "movies"
mongo = DataService.Mongo("imdb")
mongo.db[collectionName].create_index("imdbtitle", unique=True)

runType = "" #Leave this as an empty string to run all the below operations. Set to appropriate string to run just one of the below operations.

# Add all movie titles and release years into database.
# --- Most recent tested runtime (1m45s)
if runType == "" or runType == "movies":
	imdbMovies.parse(mongo, collectionName)    

# Update all movies with genre lists.
# --- Most recent tested runtime (3m10s)
if runType == "" or runType == "genres":
	imdbGenres.parse(mongo, collectionName) 

# Update all movies with lists of alternate titles.
# --- Most recent tested runtime (50s)
if runType == "" or runType == "akatitles":
	imdbAkaTitles.parse(mongo, collectionName)

# Update all movies with country information.
# --- Most recent tested runtime (3m10s)
if runType == "" or runType == "countries":
	imdbCountries.parse(mongo, collectionName) 

# Update all movies with age rating information.
# --- Most recent tested runtime (3s)
# (!!) Can we find a better MPAA list than the mpaa-ratings-reasons.list that IMDB provides?
# (!!) The IMDB list only contains ratings for ~15,000 movies, compared to the 1,000,000 something
# (!!) movies in our database.
if runType == "" or runType == "mpaa":
	imdbRatings.parse(mongo, collectionName)  

# Update all movies with production company information.
# --- Most recent tested runtime (2m30s)
if runType == "" or runType == "companies":
	imdbCompanies.parse(mongo, collectionName) 

# Update all movies with language information.
# --- Most recent tested runtime (3m10s)
if runType == "" or runType == "languages":
	imdbLanguages.parse(mongo, collectionName) 

# Update all movies with taxonomic information.
# --- Most recent tested runtime (31s)
if runType == "" or runType == "related":
	imdbRelatedFilms.parse(mongo, collectionName) 

# Collect all IMDB keywords and their respective frequencies, add it to the "keywords" collection.
# --- Most recent tested runtime (8s)
if runType == "" or runType == "keywordscollect" or runType == "keywordsfull":
	imdbKeywords.collectKeywords(mongo)

# Update all movies with keyword information.
# --- Most recent tested runtime ()
if runType == "" or runType == "keywords" or runType == "keywordsfull":
	imdbKeywords.parse(mongo, collectionName)