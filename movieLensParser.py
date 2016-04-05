import DataService
import pymongo
import movieLensRatings
import movieLensLinks
import movieLensMovies
import movieLensTags
import movieLensMoviesPopularity
import movieLensRelevance

def parseMovieLens():
    mongo = DataService.Mongo("movieLens")

    # Add all user ratings into database.
    # runtime: (172.95s)
    movieLensRatings.parse(mongo)
    mongo.db["user_rate"].create_index([("uid", pymongo.ASCENDING)])
    print("[movieLensParser] Created index for uid in user_rate")

    # Add movie id and imdb id pair into database.
    # runtime: (0.93s)
    movieLensLinks.parse(mongo)
    mongo.db["movie"].create_index([("mid", pymongo.ASCENDING)])
    print("[movieLensParser] Created index for mid in movie")

    # Add movie titles into database.
    # runtime: (3.15s)
    movieLensMovies.parse(mongo)

    # Add tags into database.
    # runtime: (0.06s)
    movieLensTags.parse(mongo)
    mongo.db["tag"].create_index([("tid", pymongo.ASCENDING)])
    print("[movieLensParser] Created index for tid in tag")

    # Add movies popularity into database.
    # runtime: (0.83s)
    movieLensMoviesPopularity.parse(mongo)

    # Add tag relevance into database.
    # runtime: (186.91s)
    movieLensRelevance.parse(mongo)


def main():
    parseMovieLens()

if __name__ == "__main__":
    main()