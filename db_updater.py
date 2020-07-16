# coding: utf-8

import json
from pymongo import MongoClient

# Establish connection
client = MongoClient('localhost', 27017)
db = client.sc2reps
collection_tgt = db.basic_sonata1_rev3


client.close()

print ("All went well")