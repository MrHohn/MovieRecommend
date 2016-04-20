# To do list:
- Analyse the keyword frequency collection, and find keywords that might be useful. We'll only store those in our database. Perhaps also try to pair IMDB keywords with keywords from the MovieLens genome dataset. (See keywordWhitelist() in imdbKeywords.py)
- For each movie, gain: movie link, image, genres, short description, popularity, average rating from imdb or movielens
- Pre-compute top rated movies and the most popular movies among all categories, and for each genres
- Pre-compute recommendations for all movielens movies (using tags or genres popularity or cosine similarity) and store result into database. Use movielens movie to narrow down the workload.
- Typeahead for tags and movies (for user searching) or use tags list and movie list --- for user adding preferences (user interaction)
- User interface building
- Gain relationship between Twitter profile and user preferences. Make recommendation based on preferences.
