import DataService
import movieLensRatings
import movieLensLinks

def main():
    mongo = DataService.Mongo("movieLens")

    # Add all user ratings into database.
    # runtime: without bulk (6m12s)
    # runtime: with bulk size 2000 (172.95s)
    movieLensRatings.parse(mongo)

    # Add movie id and imdb id pair into database.
    # runtime: (0.93s)
    movieLensLinks.parse(mongo)


if __name__ == "__main__":
    main()