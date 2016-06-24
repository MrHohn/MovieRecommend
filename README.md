# MovieRecommend
**Project Paper**: [https://www.overleaf.com/read/hbzdkxfpjhkx](https://www.overleaf.com/read/hbzdkxfpjhkx)

A movie recommender system based on Information Retrieval and Data Integration Theory. Uses content-based matching to link a user's Twitter profile contents to movie information stored in a local MongoDB instance. These results are scored using a TF-IDF style weighting system, and displayed within a GUI designed using Python's tkinter library.

Three modes of recommendation:
- **Twitter**: Recommendations based on a user's Twitter profile contents.
- **History**: Recommendations based on a list of movies specified by the user (such as a watch history)
- **Tags**: Recommendations based on a list of tags specified by the user (genres, topics, etc)

### Data sources
- [MovieLens Latest Datasets](http://grouplens.org/datasets/movielens/latest/)
- [MovieLens Tag Genome Dataset](http://grouplens.org/datasets/movielens/tag-genome/)
- [Facebook API](https://developers.facebook.com/docs/graph-api/)
- [Offline IMDB Data](http://www.imdb.com/interfaces)
- [Online IMDB API](http://www.omdbapi.com/)
- [Tweepy: Twitter for Python!](https://github.com/tweepy/tweepy)
- [Twitter REST APIs](https://dev.twitter.com/rest/public)
- [Affective Norms for English Words](http://www.uvm.edu/~pdodds/teaching/courses/2009-08UVM-300/docs/others/everything/bradley1999a.pdf)
- [TextBlob: Simplified Text Processing](https://github.com/sloria/TextBlob)
- [AYLIEN Text Analysis API](http://aylien.com/)
- [omdb.py: Python wrapper around The Open Movie Database API](https://github.com/dgilland/omdb.py)
