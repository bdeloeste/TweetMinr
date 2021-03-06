import re
import tweepy
import pymongo

CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

MONGODB_URI = "SETUP MONGODB"

AUTH = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH)

MONGO = pymongo.MongoClient(MONGODB_URI)

URL_REGEX = r'(.*) (?=http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f])))'
URL = re.compile(URL_REGEX, re.IGNORECASE | re.DOTALL)
STOP = ['rt', 'via']
