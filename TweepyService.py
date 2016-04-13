import tweepy

# Twitter interface based on Tweepy
# Tweepy repo: https://github.com/tweepy/tweepy

class Tweepy(object):

    @classmethod
    def __init__(self):
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
        print("[Twitter] Getting user profile...")
        # Get the User object from twitter...
        user = self.api.get_user(screen_name)
        profile = {}
        profile["screen_name"] = user.screen_name
        print("[Twitter] User screen_name: " + user.screen_name)
        profile["id"] = user.id
        print("[Twitter] User id: " + str(user.id))
        profile["location"] = user.location
        print("[Twitter] User location: " + user.location)
        profile["followers_count"] = user.followers_count
        print("[Twitter] User followers_count: " + str(user.followers_count))
        profile["friends_count"] = user.friends_count
        print("[Twitter] User friends_count: " + str(user.friends_count))
        profile["statuses_count"] = user.statuses_count
        print("[Twitter] User statuses_count: " + str(user.statuses_count))
        profile["profile_image_url"] = user.profile_image_url
        print("[Twitter] User profile_image_url: " + user.profile_image_url)
        profile["lang"] = user.lang
        print("[Twitter] User language: " + user.lang)
        print("[Twitter] Getting profile done.")
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
            print("[Twitter] " + str(tweet.text.encode("utf8")))
            tweet_list.append(tweet)
            tweet_count += 1
            if tweet_count >= max_count:
                break
        print("[Twitter] Retrieved tweets count: " + str(tweet_count))
        print("[Twitter] Getting tweets done.")
        return tweet_list

    @classmethod
    def get_user_following(self, screen_name):
        # reference: https://dev.twitter.com/rest/reference/get/friends/list
        friend_list = []
        friend_count = 0
        print("[Twitter] Getting user friends...")
        for user in tweepy.Cursor(self.api.friends, id=screen_name).items():
            print("[Twitter] friend's screen_name: " + user.screen_name)
            friend_list.append(user.screen_name)
            friend_count += 1
        print("[Twitter] Retrieved friends count: " + str(friend_count))
        print("[Twitter] Getting friends done.")
        return friend_list


def main():
    twitter = Tweepy()
    twitter.get_user_profile("BrunoMars")
    twitter.get_user_tweets("BrunoMars", 5) # second input is optional, default is 3200
    twitter.get_user_following("BrunoMars")

if __name__ == "__main__":
    main()