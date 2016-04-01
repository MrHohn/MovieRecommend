import DataService
import imdbMovies
import imdbAkaTitles
import imdbCountries
import imdbRatings
import imdbGenres
import imdbCompanies
import imdbLanguages
import imdbRelatedFilms

# Approximate total run time: 15 minutes

collectionName = "movies"
mongo = DataService.Mongo("imdb")
mongo.db[collectionName].create_index("imdbtitle", unique=True)

# Add all movie titles and release years into database.
# --- Most recent tested runtime (1m45s)
imdbMovies.parse(mongo, collectionName)    

# Update all movies with genre lists.
# --- Most recent tested runtime (3m10s)
imdbGenres.parse(mongo, collectionName) 

# Update all movies with lists of alternate titles.
# --- Most recent tested runtime (50s)
imdbAkaTitles.parse(mongo, collectionName)

# Update all movies with country information.
# --- Most recent tested runtime (3m10s)
imdbCountries.parse(mongo, collectionName) 

# Update all movies with age rating information.
# --- Most recent tested runtime (3s)
# (!!) Can we find a better MPAA list than the mpaa-ratings-reasons.list that IMDB provides?
# (!!) The IMDB list only contains ratings for ~15,000 movies, compared to the 1,000,000 something
# (!!) movies in our database.
imdbRatings.parse(mongo, collectionName)  

# Update all movies with production company information.
# --- Most recent tested runtime (2m30s)
imdbCompanies.parse(mongo, collectionName) 

# Update all movies with language information.
# --- Most recent tested runtime (3m10s)
imdbLanguages.parse(mongo, collectionName) 

# Update all movies with taxonomic information.
# --- Most recent tested runtime (31s)
imdbRelatedFilms.parse(mongo, collectionName) 