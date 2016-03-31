import DataService
import imdbMovies
import imdbCountries
import imdbRatings
import imdbGenres

collectionName = "movies"

mongo = DataService.Mongo("imdb")
mongo.db[collectionName].create_index("title", unique=True)

# Add all movie titles and release years into database.
# --- Most recent tested runtime (1m30s)
imdbMovies.parse(mongo, collectionName)    

# Update all movies with genre lists.
# --- Most recent tested runtime (3m02s)
imdbGenres.parse(mongo, collectionName) 

# Update all movies with country information.
# --- Most recent tested runtime (2m40s)
imdbCountries.parse(mongo, collectionName) 

# Update all movies with age rating information.
# --- Most recent tested runtime (2s)
# (!!) Can we find a better MPAA list than the mpaa-ratings-reasons.list that IMDB provides?
# (!!) The IMDB list only contains ratings for ~15,000 movies, compared to the 1,000,000 something
# (!!) movies in our database.
imdbRatings.parse(mongo, collectionName)  