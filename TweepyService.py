import tweepy

# Twitter interface based on Tweepy
# Tweepy repo: https://github.com/tweepy/tweepy

class Twitter(object):

    @classmethod
    def __init__(self):
        # Twitter App: MovieRecommend_1
        # rate limit reference: https://dev.twitter.com/rest/public/rate-limiting
        consumer_key = "9jyaCfMCKwR41rwYZ5SY5UQPm"
        consumer_secret = "Tdvg4T5dO8pIsl4u4oyBO4CxT3LNDdfyJwDfNMyeFRp4edItH1"
        access_token = "2765845462-Vdt47S1Tg7plnoD6OUkRGhzRoRJ7SgtiPozft87"
        access_token_secret = "QMXafe5KIYqWwSSMRBOPNo1sGrEn3RWD8eglCujxJlJzL"

        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

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