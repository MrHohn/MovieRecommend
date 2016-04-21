import DataService
import pymongo
import time
import re
import math
import anewParser
from textblob import TextBlob
from aylienapiclient import textapi

# Sentiment degree calculation is referring Visualizing Twitter Sentiment from NCSU
# https://www.csc.ncsu.edu/faculty/healey/tweet_viz/
# TextBlob reference: https://textblob.readthedocs.org/en/dev/index.html
# AYLIEN Text Analysis API: http://aylien.com/

class TextAnalytics(object):

    @classmethod
    def __init__(self):
        mongo = DataService.Mongo("movieRecommend")
        anewDoc = mongo.db["anew"].find_one({"type": "all"})
        if anewDoc is None:
            anewParser.parse(mongo)
            anewDoc = mongo.db["anew"].find_one({"type": "all"})            
        self.anewDict = anewDoc["dict"]
        print("[Sentiment] ANEW list retrieved.")
        self.textapi = textapi.Client("YOUR_APP_ID", "YOUR_APP_KEY")
        print("[Aylien] Aylien client initialized.")

    @classmethod
    def gain_sentiment(self, sentence):
        words = self.tokenize(sentence)
        if len(words) < 2:
            return 0
        valence_list = []
        for word in words:
            if word in self.anewDict:
                valence_list.append([self.anewDict[word]["valence_mean"], self.anewDict[word]["valence_sd"]])

        if len(valence_list) < 2:
            return 0

        weight_sum = 0
        value_sum = 0
        for cur in valence_list:
            cur_weight = self.probability_density(cur[1])
            weight_sum += cur_weight
            value_sum += cur[0] * cur_weight
        return value_sum / weight_sum

    @classmethod
    # https://en.wikipedia.org/wiki/Probability_density_function
    def probability_density(self, sd):
        return 1 / (sd * math.sqrt(2 * math.pi))

    # unfinished
    @classmethod
    def tokenize(self, sentence):
        # words = re.split("\s|\.|;|,|\*|\n|!|'|\"", sentence)
        text = TextBlob(sentence)
        words = text.words
        res = []
        for word in words:
            if len(word) > 0:
                res.append(self.stemming(word))
        # print(res)
        return res

    # unfinished
    # handle empty or invalid word, and stem word
    @classmethod
    def stemming(self, word):
        return word.lower()

    @classmethod
    def classify(self, profile):
        print("[Twitter] Getting classification from user profile...")
        user_tweets = ""
        for tweet in profile["extracted_tweets"]:
            user_tweets += "\n" + tweet
            # print(tweet.encode("utf8"))

        classification = self.textapi.Classify({"text": user_tweets})
        return classification


def main():
    textAnalytics = TextAnalytics()
    # sentence = "Congrats to @HCP_Nevada on their health care headliner win"
    # sentence = "b'I love you @iHeartRadio! I love you hooligans! love you Sriracha. I love you @LeoDiCaprio. Thinking of u @holyfield  https://t.co/iPoHf03G4R'"
    sentence = "The secret life of Walter Mitty is a fantastic movie"
    print("[TweetAnalytics] Evaluating sentence: " + sentence)
    score = textAnalytics.gain_sentiment(sentence)
    print("[TweetAnalytics] Sentiment score: " + str(score))

if __name__ == "__main__":
    main()