import DataService
import movieLensRatings

def main():
    mongo = DataService.Mongo("movieRecommend")

    # Add all user ratings into database.
    # runtime: without bulk (6m12s)
    # runtime: with bulk size 1000 (172.78s)
    # runtime: with bulk size 2000 (172.95s)
    movieLensRatings.parse(mongo)


if __name__ == "__main__":
    main()