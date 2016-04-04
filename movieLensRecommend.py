import DataService
import pymongo
import time
import math
import queue

# Recommender based on MovieLens database
# db name: movieLens

class MovieLensRecommend(object):

    @classmethod
    # return top-10 similar users given an User ID
    def get_similar_user(self, userID):
        mongo = DataService.Mongo("movieLens")
        target_user = mongo.db["user_rate"].find_one({"uid": userID})
        target_id = target_user["uid"]
        target_like = set()
        for rating in target_user["ratings"]:
            if rating[1] >= 3.5:
                target_like.add(rating[0])
        if len(target_like) < 5:
            print("[MovieLensRecommend] Not enough rating history: " + str(len(target_like)) + ".")
            return
        print("[MovieLensRecommend] Sufficient history: " + str(len(target_like))+ ", now start calculating.")

        progressInterval = 10000  # How often should we print a progress report to the console?
        progressTotal = 276961    # Approximate number of total users
        count = 0                 # Counter for progress

        # Scan through all users in database and calculate similarity
        startTime = time.time()
        # maintain a min heap for top k candidates
        candidates = queue.PriorityQueue()
        cursor = mongo.db["user_rate"].find({})
        for cur_user in cursor:
            count += 1
            if count % progressInterval == 0:
                print("[movieLensLikeDislike] " + str(count) + " users processed so far. (" + str(int(count * 100 / progressTotal)) + "%%) (%0.2fs)" % (time.time() - startTime))

            cur_id = cur_user["uid"]
            if cur_id == target_id:
                continue

            cur_like = set()
            for rating in cur_user["ratings"]:
                if rating[1] >= 3.5:
                    cur_like.add(rating[0])
            if len(cur_like) < 5:
                continue
            cur_similarity = self.cosineSimilarity(cur_like, target_like)
            candidates.put(User(cur_similarity, cur_id))
            # maintain the size
            if candidates.qsize() > 10:
                candidates.get()

        # now print out top 10 candidate
        while not q.empty():
            cur_user = q.get()
            print("uid: " + str(cur_user.uid) + ", score: " + str(cur_user.score))

    @classmethod
    def cosineSimilarity(self, set1, set2):
        match_count = self.countMatch(set1, set2)
        return float(match_count) / math.sqrt(len(set1) * len(set2))

    @classmethod
    def countMatch(self, set1, set2):
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


def main():
    # unit test, input: User ID = 4
    MovieLensRecommend.get_similar_user(4)
    

if __name__ == "__main__":
    main()