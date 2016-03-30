import DataService
import imdbMovies

collectionName = "movies"

mongo = DataService.Mongo("imdb")
mongo.db[collectionName].create_index("title", unique=True)

imdbMovies.parse(mongo, collectionName) #Add all movie titles into database