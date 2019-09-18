import csv
import json
import pandas as pd
import sys, getopt, pprint
from pymongo import MongoClient
import pymongo
#CSV to JSON Conversion
csvfile = open('ETL_names_file3.csv', 'r', encoding="iso-8859-1")
reader = csv.DictReader( csvfile )
mongo_client=MongoClient('localhost', 27020) 
header= ["Index","AlbumName",'Artist',"Explicit",'Genre','TrackCount']
db=mongo_client.Artist_Database
coll = db.artists
coll.create_index('Index',unique=True)
for each in reader:
    row={}
    for field in header:
        row[field]=each[field]
    coll.insert_one(row)
