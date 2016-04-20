import DataService
import pymongo
import time
import math
import queue
from TwitterService import Tweepy
from aylienapiclient import textapi
from TweetAnalytics import TextAnalytics

# Recommender based on MovieLens database
# db name: movieLens

class MovieLensRecommend(object):

    @classmethod
    def __init__(self):
        self.mongo = DataService.Mongo("movieLens")
        self.mongoTwitter = DataService.Mongo("movieRecommend")

    # unfinished
    @classmethod
    def recommend_movies_for_twitter(self, screen_name):
        print("[MovieLensRecommend] Target user screen_name: " + screen_name)

        profile = self.mongoTwitter.db["user_profiles"].find_one({"screen_name": screen_name})
        if profile is None:
            print("[MovieLensRecommend] Not found in database.")
            twitter = Tweepy()
            profile = twitter.extract_profile(screen_name)

        print("[MovieLensRecommend] Profile retrieved.")
        textAnalytics = TextAnalytics()
        classification = textAnalytics.classify(profile)
        print("[MovieLensRecommend] Classified: " + str(classification["categories"]))
        # TODO

    # generate up to 10 movies recommendations given a movie id
    # the core idea is cosine similarity between tags list
    @classmethod
    def recommend_movies_for_movie(self, mid):
        print("[MovieLensRecommend] Target movie id: " + str(mid))
        target_movie = self.mongo.db["movie"].find_one({"mid": mid})
        if "similar_movies" in target_movie:
            print("[MovieLensRecommend] Similar movies calculated.")
            most_similar_movies = target_movie["similar_movies"]
        else:
            print("[MovieLensRecommend] Similar movies not calculated.")
            if "tags" not in target_movie:
                print("[MovieLensRecommend] No tagging info found, unable to recommend.")
                return
            print("[MovieLensRecommend] Tagging info found, now start calculating...")
            target_mid = mid
            target_tags_score = target_movie["tags"]
            target_tags = set()
            for tag_score in target_tags_score:
                attrs = tag_score.split(",")
                target_tags.add(int(attrs[0]))

            progressInterval = 2000   # How often should we print a progress report to the console?
            progressTotal = 34028     # Approximate number of total users
            count = 0                 # Counter for progress

            # Scan through all movies in database and calculate similarity
            startTime = time.time()
            # maintain a min heap for top k candidates
            candidates = queue.PriorityQueue()
            cursor = self.mongo.db["movie"].find({})
            for cur_movie in cursor:
                count += 1
                if count % progressInterval == 0:
                    print("[MovieLensRecommend] %6d movies processed so far. (%d%%) (%0.2fs)" % ((count, int(count * 100 / progressTotal), time.time() - startTime)))

                # skip non-tagged movie
                if "tags" not in cur_movie:
                    continue

                cur_id = cur_movie["mid"]
                if cur_id == target_mid:
                    continue

                cur_tags_score = cur_movie["tags"]
                cur_tags = set()
                for tag_score in cur_tags_score:
                    attrs = tag_score.split(",")
                    cur_tags.add(int(attrs[0]))

                cur_similarity = self.cosine_similarity(cur_tags, target_tags)
                candidates.put(Candidate(cur_id, cur_similarity))
                # maintain the pool size
                if candidates.qsize() > 10:
                    candidates.get()

            # now print out and return top 10 candidates
            most_similar_movies = []
            while not candidates.empty():
                cur_movie = candidates.get()
                most_similar_movies.append(cur_movie.cid)
                # print("[MovieLensRecommend] uid: " + str(cur_movie.cid) + ", score: " + str(cur_movie.score))
            print("[MovieLensRecommend] Calculation complete.")
            self.mongo.db["movie"].update_one({"mid": target_mid}, {"$set": {"similar_movies": most_similar_movies}}, True)
            print("[MovieLensRecommend] Stored similar movies into DB.")

        self.print_recommend(most_similar_movies)
        return most_similar_movies

    # generate up to 20 movies recommendations given a list of tags
    # the core idea is tf.idf weight (content-based query)
    @classmethod
    def recommend_movies_based_on_tags(self, tags):
        print("[MovieLensRecommend] Target tags: " + str(tags))
        total_movies_num = 9734
        movies_score = {}
        for tid in tags:
            cur_tag = self.mongo.db["tag"].find_one({"tid": tid})
            cur_popular = cur_tag["popular"]
            cur_movies = cur_tag["relevant_movie"]
            for relevance_pair in cur_movies:
                attrs = relevance_pair.split(",")
                mid = int(attrs[0])
                relevance = float(attrs[1])
                score = self.weight_tf_idf(relevance, cur_popular, total_movies_num)
                if mid not in movies_score:
                    movies_score[mid] = score
                else:
                    movies_score[mid] += score

        # put all candidates to compete, gain top-k
        recommend = self.gain_top_k(movies_score, 20)
        self.print_recommend(recommend)
        return recommend

    @classmethod
    def weight_tf_idf(self, tf, df, num_docs):
        return tf * math.log(float(num_docs) / df, 2)

    # gain top-k candidates given a dictionary storing their scores
    @classmethod
    def gain_top_k(self, candidates, k):
        candidates_pool = queue.PriorityQueue()
        for cid in candidates:
            candidates_pool.put(Candidate(cid, candidates[cid]))
            # maintain the size
            if candidates_pool.qsize() > k:
                candidates_pool.get()

        top_k = []
        while not candidates_pool.empty():
            cur_candidate = candidates_pool.get()
            top_k.append(cur_candidate.cid)
            # print("[MovieLensRecommend] Candidate id: " + str(cur_candidate.cid) + ", score: " + str(cur_candidate.score))
        top_k.reverse()
        return top_k

    # given a list of recommendations, print out the movies information
    @classmethod
    def print_recommend(self, recommend):
        print("[MovieLensRecommend] - Recommend movies: -")
        for movie_id in recommend:
            movie_data = self.mongo.db["movie"].find_one({"mid": movie_id})
            print("[MovieLensRecommend] imdbid: %7d, %s" % (movie_data["imdbid"],movie_data["title"]))
        print("[MovieLensRecommend] - Recommend complete. -")

    # generate up to 20 movies recommendations given an User ID
    # the core idea is collaborative filtering (comparing movie occurrences)
    @classmethod
    def recommend_movies_for_user(self, userID):
        target_user = self.mongo.db["user_rate"].find_one({"uid": userID})
        print("[MovieLensRecommend] Target user ID: [" + str(userID) + "]")
        # check if similar users were calculated
        if "similar_users" in target_user:
            print("[MovieLensRecommend] Similar users calculated.")
            most_similar_users = target_user["similar_users"]
        else:
            print("[MovieLensRecommend] Similar users not calculated.")
            most_similar_users = self.get_similar_users(userID)
        print("[MovieLensRecommend] Similar users retrieved.")
        print("[MovieLensRecommend] Start generating recommend movies...")
        # gain target user history
        target_history = set()
        for rating in target_user["ratings"]:
            target_history.add(rating[0])

        movies_count = {}
        for cur_user_id in most_similar_users:
            cur_user = self.mongo.db["user_rate"].find_one({"uid": cur_user_id})
            for rating in cur_user["ratings"]:
                # if user like this movie
                if rating[1] >= 3.5:
                    # and the movie is not rated by target user
                    if rating[0] not in target_history:
                        # count occurrences
                        if rating[0] not in movies_count:
                            movies_count[rating[0]] = 1
                        else:
                            movies_count[rating[0]] += 1

        # put all occurrences in to heap to gain top-k
        recommend = self.gain_top_k(movies_count, 20)
        self.print_recommend(recommend)
        return recommend

    # return top-10 similar users given an User ID
    # the core idea is cosine similarity between user like list
    @classmethod
    def get_similar_users(self, userID):
        print("[MovieLensRecommend] Start calculating similar users...")
        target_user = self.mongo.db["user_rate"].find_one({"uid": userID})
        target_id = userID
        target_like = set()
        for rating in target_user["ratings"]:
            if rating[1] >= 3.5:
                target_like.add(rating[0])
        if len(target_like) < 5:
            print("[MovieLensRecommend] Not enough rating history: " + str(len(target_like)) + ".")
            return
        print("[MovieLensRecommend] Sufficient history: " + str(len(target_like))+ ", now start calculating...")

        progressInterval = 10000  # How often should we print a progress report to the console?
        progressTotal = 247753    # Approximate number of total users
        count = 0                 # Counter for progress

        # Scan through all users in database and calculate similarity
        startTime = time.time()
        # maintain a min heap for top k candidates
        candidates = queue.PriorityQueue()
        cursor = self.mongo.db["user_rate"].find({})
        for cur_user in cursor:
            count += 1
            if count % progressInterval == 0:
                print("[MovieLensRecommend] %6d lines processed so far. (%d%%) (%0.2fs)" % ((count, int(count * 100 / progressTotal), time.time() - startTime)))

            cur_id = cur_user["uid"]
            if cur_id == target_id:
                continue

            cur_like = set()
            for rating in cur_user["ratings"]:
                if rating[1] >= 3.5:
                    cur_like.add(rating[0])
            if len(cur_like) < 5:
                continue
            cur_similarity = self.cosine_similarity(cur_like, target_like)
            candidates.put(Candidate(cur_id, cur_similarity))
            # maintain the pool size
            if candidates.qsize() > 10:
                candidates.get()

        # now print out and return top 10 candidates
        most_similar_users = []
        while not candidates.empty():
            cur_user = candidates.get()
            most_similar_users.append(cur_user.cid)
            # print("[MovieLensRecommend] uid: " + str(cur_user.cid) + ", score: " + str(cur_user.score))
        print("[MovieLensRecommend] Calculation complete (%0.2fs)" % (time.time() - startTime))
        self.mongo.db["user_rate"].update_one({"uid": target_id}, {"$set": {"similar_users": most_similar_users}}, True)
        print("[MovieLensRecommend] Stored similar users into DB.")
        return most_similar_users

    @classmethod
    def cosine_similarity(self, set1, set2):
        match_count = self.count_match(set1, set2)
        return float(match_count) / math.sqrt(len(set1) * len(set2))

    @classmethod
    def count_match(self, set1, set2):
        count = 0
        for element in set1:
            if element in set2:
                count += 1
        return count

class Candidate(object):
    def __init__(self, cid, score):
        self.cid = cid
        self.score = score

    # less than interface, __cmp__ is gone in Python 3.4?
    def __lt__(self, other):
        return self.score < other.score


def main():
    recommend = MovieLensRecommend()

    # # unit test, input: User ID = 4
    # print("[MovieLensRecommend] ***** Unit test for recommend_movies_for_user() *****")
    # user_id = 4
    # recommend.recommend_movies_for_user(user_id)

    # # unit test, input:
    # # 28:  adventure
    # # 387: feel-good
    # # 599: life
    # # 704: new york city
    # # 794: police
    # print("[MovieLensRecommend] ***** Unit test for recommend_movies_based_on_tags() *****")
    # tags = [28, 387, 599, 704, 794]
    # recommend.recommend_movies_based_on_tags(tags)

    # # unit test, input: Movie ID = 1 "Toy Story (1995)"
    # print("[MovieLensRecommend] ***** Unit test for recommend_movies_for_movie() *****")
    # movie_id = 1
    # recommend.recommend_movies_for_movie(movie_id)

    recommend.recommend_movies_for_twitter("BrunoMars")

if __name__ == "__main__":
    main()