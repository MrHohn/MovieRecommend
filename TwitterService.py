from DataService import Mongo
import pymongo
import tweepy
import time

# Twitter interface based on Tweepy
# Tweepy repo: https://github.com/tweepy/tweepy

class Tweepy(object):

    @classmethod
    def __init__(self, mongo):
        self.mongo = mongo
        # create Twitter Apps: https://apps.twitter.com/
        consumer_key = ""
        consumer_secret = ""
        access_token = ""
        access_token_secret = ""

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        self.api = tweepy.API(auth)
        print("[Twitter] Tweepy initialized.")

    @classmethod
    def get_rate_limit(self):
        # reference: https://dev.twitter.com/rest/public/rate-limiting
        rate_limit = self.api.rate_limit_status()
        print(rate_limit)

    @classmethod
    def get_user_profile(self, screen_name):
        print("[Twitter] Getting user basic profile...")
        # Get the User object from twitter...
        user = self.api.get_user(screen_name)
        profile = {}
        profile["screen_name"] = user.screen_name
        profile["name"] = user.name
        profile["id"] = user.id
        profile["location"] = user.location
        profile["followers_count"] = user.followers_count
        profile["friends_count"] = user.friends_count
        profile["statuses_count"] = user.statuses_count
        profile["profile_image_url"] = user.profile_image_url
        profile["lang"] = user.lang
        return profile

    @classmethod
    def get_user_tweets(self, screen_name, max_count=3200):
        # reference: https://dev.twitter.com/rest/reference/get/statuses/user_timeline
        # This method can only return up to 3,200 of a user’s most recent Tweets. Native retweets of other statuses by the user is included in this total, regardless of whether include_rts is set to false when requesting this resource.
        # 200 is the upper bound for one time retrieval (cursor would handle pagination)
        print("[Twitter] Getting user tweets...")
        tweet_list = []
        tweet_count = 0
        for tweet in tweepy.Cursor(self.api.user_timeline, id=screen_name, count=200, include_rts=False).items():
            tweet_list.append(tweet)
            tweet_count += 1
            if tweet_count >= max_count:
                break
        print("[Twitter] Retrieved tweets count: " + str(tweet_count))
        return tweet_list

    @classmethod
    def get_user_hashtags(self, tweet_list):
        all_tags = []
        for tweet in tweet_list:
            cur_tags = tweet.entities["hashtags"]
            for tag in cur_tags:
                # print(tag)
                all_tags.append(tag["text"])
        return all_tags

    @classmethod
    def get_user_mentions(self, tweet_list):
        all_mentions = []
        for tweet in tweet_list:
            cur_mentions = tweet.entities["user_mentions"]
            for mention in cur_mentions:
                # print(mention)
                all_mentions.append(mention)
        return all_mentions

    @classmethod
    def get_user_followings(self, screen_name, max_count=2000):
        # reference: https://dev.twitter.com/rest/reference/get/friends/list
        # The number of users to return per page is up to a maximum of 200
        # the rate limit is 15 calss per 15 minutes
        # hence we set up a reasonable limitation: max_count = 2000
        friend_list = []
        friend_count = 0
        print("[Twitter] Getting user friends...")
        for user in tweepy.Cursor(self.api.friends, id=screen_name, count=200).items():
            friend_list.append(user)
            friend_count += 1
            if friend_count >= max_count:
                break
        print("[Twitter] Retrieved friends count: " + str(friend_count))
        return friend_list

    @classmethod
    def get_suggested_categories(self, screen_name):
        # reference: https://dev.twitter.com/rest/reference/get/users/suggestions
        print("[Twitter] Getting user suggested categories...")
        user = self.api.get_user(screen_name)
        categories = user._api.suggested_categories()
        res = []
        for cate in categories:
            res.append(cate.slug)
        return res

    @classmethod
    def get_suggested_users(self, screen_name, category):
        # reference: https://dev.twitter.com/rest/reference/get/users/suggestions/%3Aslug
        print("[Twitter] Getting user suggested users...")
        user = self.api.get_user(screen_name)
        return user._api.suggested_users(slug=category)

    @classmethod
    def get_user_likes(self, screen_name, max_count=1000):
        # reference: https://dev.twitter.com/rest/reference/get/favorites/list
        print("[Twitter] Getting user likes...")
        like_list = []
        like_count = 0
        # count: Specifies the number of records to retrieve. Must be less than or equal to 200; defaults to 20.
        for like in tweepy.Cursor(self.api.favorites, id=screen_name, count=200, include_rts=False).items():
            like_list.append(like)
            like_count += 1
            if like_count >= max_count:
                break
        print("[Twitter] Retrieved likes count: " + str(like_count))
        return like_list

    @classmethod
    def print_basic_profile(self, profile):
        print("[Twitter] User screen_name: " + profile["screen_name"])
        print("[Twitter] User name: " + profile["name"])
        print("[Twitter] User id: " + str(profile["id"]))
        print("[Twitter] User location: " + profile["location"])
        print("[Twitter] User followers_count: " + str(profile["followers_count"]))
        print("[Twitter] User friends_count: " + str(profile["friends_count"]))
        print("[Twitter] User statuses_count: " + str(profile["statuses_count"]))
        print("[Twitter] User profile_image_url: " + profile["profile_image_url"])
        print("[Twitter] User language: " + profile["lang"])

    @classmethod
    def extract_profile(self, screen_name):
        print("[Twitter] Start Twitter Profile extraction.")
        print("[Twitter] Target user: " + screen_name)
        startTime = time.time()

        # unit test for get_user_profile(screen_name)
        profile = self.get_user_profile(screen_name)
        # self.print_basic_profile(profile)

        extracted_tags = []
        extracted_tweets = []
        extracted_users = []

        # unit test for get_user_tweets(screen_name, count)
        tweet_list = self.get_user_tweets(screen_name) # second argument is optional, default is 3,200
        for tweet in tweet_list:
            extracted_tweets.append(tweet.text)
            # print("[Twitter] Tweet: " + str(tweet.text.encode("utf8")))

        # unit test for get_user_hashtags(tweet_list)
        hashtags = self.get_user_hashtags(tweet_list)
        for tag in hashtags:
            extracted_tags.append(tag)
            # print("[Twitter] Tag: " + tag)

        # unit test for get_user_mentions(tweet_list)
        mentions = self.get_user_mentions(tweet_list)
        for mention in mentions:
            extracted_users.append([mention["screen_name"], mention["name"]])
            # print("[Twitter] Mention screen_name: " + mention["screen_name"])

        # unit test for get_user_likes(screen_name, count)
        like_list = self.get_user_likes(screen_name) # second argument is optional, default is 1,000
        for like in like_list:
            extracted_tweets.append(like.text)
            # print("[Twitter] Like: " + str(like.text.encode("utf8")))

        # unit test for get_user_hashtags(tweet_list)
        hashtags = self.get_user_hashtags(like_list)
        for tag in hashtags:
            extracted_tags.append(tag)
            # print("[Twitter] Tag: " + tag)

        # unit test for get_user_mentions(tweet_list)
        mentions = self.get_user_mentions(like_list)
        for mention in mentions:
            extracted_users.append([mention["screen_name"], mention["name"]])
            # print("[Twitter] Mention screen_name: " + mention["screen_name"])

        # unit test for get_user_followings(screen_name)
        followings = self.get_user_followings(screen_name)
        for following in followings:
            extracted_users.append([following.screen_name, following.name])
            # print("[Twitter] Friend's screen_name: " + following.screen_name)

        # Abandoned, the suggestions are too general
        # # unit test for get_suggested_categories(screen_name)
        # categories = self.get_suggested_categories(screen_name)
        # profile["suggested_categories"] = categories
        # # for category in categories:
        # #     print("[Twitter] Suggested category: " + category)

        # # unit test for get_suggested_users(screen_name)
        # users = self.get_suggested_users(screen_name, "Television")
        # for user in users:
        #     print("[Twitter] Suggested user: " + user.name)

        profile["extracted_tags"] = extracted_tags
        profile["extracted_tweets"] = extracted_tweets
        profile["extracted_users"] = extracted_users

        print("[Twitter] Inserting user profile into database...")
        self.mongo.db["user_profiles"].insert_one(profile)
        self.mongo.db["user_profiles"].create_index([("screen_name", pymongo.ASCENDING)])
        print("[Twitter] Created index for screen_name in user_profiles")
        print("[Twitter] Extraction done (%0.2fs)." % (time.time() - startTime))
        return profile


def main():
    twitter = Tweepy(Mongo("movieRecommend"))
    twitter.get_rate_limit()
    # twitter.extract_profile("LeoDiCaprio")
    # twitter.extract_profile("BrunoMars")
    # cats = twitter.get_suggested_categories("BrunoMars")
    # cats = twitter.get_suggested_categories("LeoDiCaprio")
    # cats = twitter.get_suggested_categories("johnydepp007")
    # cats = twitter.get_suggested_categories("ZihongZ")
    # print(cats)

if __name__ == "__main__":
    main()