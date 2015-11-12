import re
import tweepy
import pymongo

CONSUMER_KEY = "PBr0V80Bs9FrBApLaAhXTNYpK"
CONSUMER_SECRET = "KXyjb2O747zLUSF27raWculoF1QLcDxB5zoNiprN6OPGN86yqB"
ACCESS_TOKEN = "434673129-EFPjgCByhu6zku31HPd5TgMaWGmIEkyKjpBDDUHw"
ACCESS_TOKEN_SECRET = "p0RvEMbgNdv6G1aF3B9y2VTnMw39h3LLNnwrY0Fwg0czw"

MONGODB_URI = "mongodb://54.175.3.211:27017/"

AUTH = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
AUTH.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
API = tweepy.API(AUTH)

MONGO = pymongo.MongoClient(MONGODB_URI)

URL_REGEX = r'(.*) (?=http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f])))'
URL = re.compile(URL_REGEX, re.IGNORECASE | re.DOTALL)
STOP = ['rt', 'via']