import DataService
import pymongo
import time
import math
import queue

# Recommender based on MovieLens database
# db name: movieLens

class MovieLensRecommend(object):
    
    # generate up to 20 movies recommendations given an User ID
    @classmethod
    def recommend_movies_for_user(self, userID):
        mongo = DataService.Mongo("movieLens")
        target_user = mongo.db["user_rate"].find_one({"uid": userID})
        print("[MovieLensRecommend] Target user ID: [" + str(userID) + "]")
        # check if similar users were calculated
        if "similar_users" in target_user:
            print("[MovieLensRecommend] Similar users calculated.")
            most_similar_users = target_user["similar_users"]
        else:
            print("[MovieLensRecommend] Similar users not calculated.")
            most_similar_users = self.get_similar_users(userID, mongo)
        print("[MovieLensRecommend] Similar users retrieved.")
        print("[MovieLensRecommend] Start generating recommend movies...")
        # gain target user history
        target_history = set()
        for rating in target_user["ratings"]:
            target_history.add(rating[0])

        movies_count = {}
        for cur_user_id in most_similar_users:
            cur_user = mongo.db["user_rate"].find_one({"uid": cur_user_id})
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
        movies_pool = queue.PriorityQueue()
        for movie_id in movies_count:
            movies_pool.put(Movie(movies_count[movie_id], movie_id))
            # maintain the size
            if movies_pool.qsize() > 20:
                movies_pool.get()

        recommend = []
        while not movies_pool.empty():
            cur_movie = movies_pool.get()
            recommend.append(cur_movie.mid)
            # print("[MovieLensRecommend] Recommend movie ID: " + str(cur_movie.mid) + ", occurrences: " + str(cur_movie.count))

        print("[MovieLensRecommend] -------- Recommend movies --------")
        for movie_id in reversed(recommend):
            movie_data = mongo.db["movie"].find_one({"mid": movie_id})
            print("[MovieLensRecommend] imdbid: %6d, %s" % (movie_data["imdbid"],movie_data["title"]))
        print("[MovieLensRecommend] -------- Recommend complete --------")
        return recommend

    # return top-10 similar users given an User ID
    @classmethod
    def get_similar_users(self, userID, mongo):
        print("[MovieLensRecommend] Start calculating similar users...")
        target_user = mongo.db["user_rate"].find_one({"uid": userID})
        target_id = target_user["uid"]
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
        cursor = mongo.db["user_rate"].find({})
        for cur_user in cursor:
            count += 1
            if count % progressInterval == 0:
                print("[MovieLensRecommend] " + str(count) + " users processed so far. (" + str(int(count * 100 / progressTotal)) + "%%) (%0.2fs)" % (time.time() - startTime))

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
            candidates.put(User(cur_similarity, cur_id))
            # maintain the pool size
            if candidates.qsize() > 10:
                candidates.get()

        # now print out and return top 10 candidates
        most_similar_users = []
        while not candidates.empty():
            cur_user = candidates.get()
            most_similar_users.append(cur_user.uid)
            # print("[MovieLensRecommend] uid: " + str(cur_user.uid) + ", score: " + str(cur_user.score))
        print("[MovieLensRecommend] Calculation complete.")
        mongo.db["user_rate"].update_one({"uid": target_id}, {"$set": {"similar_users": most_similar_users}}, True)
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

class User(object):
    def __init__(self, score, uid):
        self.score = score
        self.uid = uid

    # less than interface, __cmp__ is gone in Python 3.4?
    def __lt__(self, other):
        return self.score < other.score

class Movie(object):
    def __init__(self, count, mid):
        self.count = count
        self.mid = mid

    # less than interface, __cmp__ is gone in Python 3.4?
    def __lt__(self, other):
        return self.count < other.count


def main():
    # unit test, input: User ID = 4
    MovieLensRecommend.recommend_movies_for_user(4)

if __name__ == "__main__":
    main()