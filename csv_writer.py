'''
	File Name: crawler.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script pulls trips from the database, checks for errors, write trips to a JSON
		file and then output to a CSV file.
'''

# Import libraries.
import os
import csv
import json
import requests
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps

#-----------------------------------------------------------------------#
#							Function: Make CSV 							#
#-----------------------------------------------------------------------#
def make_csv(week,day):
	# Open Log and add details.
	logger = logging.getLogger("normal_crawler.csv_writer")
	logger.info("Creating CSV file for week: "+str(week)+"day "+str(day))

	# Create file name.
	MAIN_NAME = "normal-crawler-"
	INCREMENTAL_FILENAME_SUFFIX = str(week)+"-"+str(day)
	NAME_EXTENSION = ".csv"
	OUTPUT_DIR = "/data/Congestion/stream/normal-crawler/"
	FINAL_NAME = OUTPUT_DIR+MAIN_NAME+INCREMENTAL_FILENAME_SUFFIX+NAME_EXTENSION

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Find all trips for the given day of the week.
	r = db.try0.find({"weeks":week,"timestamp.day":day})
	l = list(r)
	length = len(l)
	logger.info("Number of trips to write to csv today: "+str(length))

	# Initialize number of error crawls.
	error_crawls = 0

	# Loop through all trips to replace with blanks for errors.
	for num in range(length):
		if str(l[num]["driving"]["distance"]) == "-3" or str(l[num]["driving"]["traffic"]) == "-3" or str(l[num]["driving"]["distance"]) == "-2" or str(l[num]["driving"]["traffic"]) == "-2" or str(l[num]["driving"]["distance"]) == "-1" or str(l[num]["driving"]["traffic"]) == "-1":
			error_crawls = error_crawls+1
			l[num]["driving"]["distance"] = " "
			l[num]["driving"]["traffic"] = " "
			l[num]["driving"]["time"] = " "

	# Send number of errors to slack.
	url = "https://hooks.slack.com/services/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"
	err_msg = "There were " + str(error_crawls) + " errors today."
	payload1={"text": err_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
		logger.info(str(err_msg))
	except requests.exceptions.RequestException as e:
		logger.info("Error while sending Slack Notification 1")
		logger.info(str(e))
		logger.info(str(err_msg))

	# Write all JSON trips to file.
	ljson = dumps(l,sort_keys = True, indent = 4, separators = (',',':'))
	f = open('normal_crawler_todays_trips_from_db.json', 'w')  
	f.write(ljson)
	f.close()

	# Read JSON file.
	file = open('normal_crawler_todays_trips_from_db.json','r')
	x = json.loads(file.read())

	# Open and write to CSV file.
	f = open(FINAL_NAME, "ab+")
	z = csv.writer(f)
	z.writerow(["city","survey","trip_id","weeks","origin_latitude","origin_longitude","destination_latitude","destination_longitude","timestamp_week","timestamp_day","timestamp_hours","timestamp_minutes","distance","time","traffic"])
	for index in x:
	    z.writerow([index["city"],index["survey"],index["trip_id"],index["weeks"],index["origin"]["latitude"],index["origin"]["longitude"],index["destination"]["latitude"],index["destination"]["longitude"],index["timestamp"]["week"],index["timestamp"]["day"],index["timestamp"]["hours"],index["timestamp"]["minutes"],index["driving"]["distance"],index["driving"]["time"],index["driving"]["traffic"]])
	f.close()

	# Send Slack notification after successfully writing CSV file.
	csv_msg = "Sao Paulo 2012 Survey Normal-Crawler: CSV for week-"+str(week)+"-day-"+str(day)+" has been written successfully to the shared drive."
	payload1={"text": csv_msg}
	try:
		r = requests.post(url, data=json.dumps(payload1))
	except requests.exceptions.RequestException as e:
		logger.info(str("Error while sending Slack Notification 2"))
		logger.info(str(e))
		logger.info(str(csv_msg))
