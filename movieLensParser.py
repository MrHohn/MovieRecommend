import DataService
import movieLensRatings
import movieLensLinks
import movieLensMovies

def parseMovieLens():
    mongo = DataService.Mongo("movieLens")

    # Add all user ratings into database.
    # runtime: (172.95s)
    movieLensRatings.parse(mongo)

    # Add movie id and imdb id pair into database.
    # runtime: (0.93s)
    movieLensLinks.parse(mongo)

    # Add movie titles into database.
    # runtime: (3.15s)
    movieLensMovies.parse(mongo)


def main():
    parseMovieLens()

if __name__ == "__main__":
    main()