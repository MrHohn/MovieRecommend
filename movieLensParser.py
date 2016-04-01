import DataService
import movieLensRatings

def main():
    mongo = DataService.Mongo("movieRecommend")

    # Add all user ratings into database.
    # --- Most recent tested runtime (6m12s)
    movieLensRatings.parse(mongo)


if __name__ == "__main__":
    main()