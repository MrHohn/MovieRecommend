import tweepy

# Twitter interface based on Tweepy
# Tweepy repo: https://github.com/tweepy/tweepy

class Twitter(object):

    @classmethod
    def __init__(self):
        # create Twitter Apps: https://apps.twitter.com/
        consumer_key = ""
        consumer_secret = ""
        access_token = ""
        access_token_secret = ""

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        # rate limit reference: https://dev.twitter.com/rest/public/rate-limiting
        self.api = tweepy.API(auth)
        print("[Twitter] Tweepy initialized.")

    @classmethod
    def get_rate_limit(self):
        return self.api.rate_limit_status()


def main():
    twitter = Twitter()
    rate_limit = twitter.get_rate_limit()
    print(rate_limit)
    # print user_timeline limit
    print("\nuser_timeline limit:")
    print(rate_limit["resources"]["statuses"]["/statuses/user_timeline"])

    # Get the User object for twitter...
    user = twitter.api.get_user("BarackObama")

    print("\nscreen name: " + user.screen_name)
    print("followers count: " + str(user.followers_count))
    print("twitter id: " + str(user.id))
    for friend in user.friends():
       print("friend's screen_name: " + friend.screen_name)

    private_tweets = user.timeline(count=200, include_rts=False)
    for tweet in private_tweets:
        print(tweet.text.encode("utf-8"))

if __name__ == "__main__":
    main()