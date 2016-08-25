'''
	File Name: scheduler.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script pulls data for the week from the database. The trips are converted to 
		brazil time and scheduled for crawls. The crawler is called for every trip. The call 
		to the CSV creator is scheduled at 6:01am server time, 1:01am central time everyday 
		for the days trips. The script then returns control to the controller on Saturday at 
		7:00am server time, 2:00am central time.
'''

# Import libraries.
import crawler
import csv_writer
import schedule
import datetime
import requests
import logging
import time
import json
import copy
import os
from bson.objectid import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient

# Global variable to keep track of scheduling loop.
schd_bool = 0

#-----------------------------------------------------------------------#
#						Function: Schedule Trips						#
#-----------------------------------------------------------------------#
def schedule_trips(week):
	# Open Log.
	logger = logging.getLogger("normal_crawler.scheduler")
	logger.info("Scheduling trips for week: "+str(week))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0

	# Resetting global variable to 0, from week 2.
	global schd_bool
	schd_bool = 0

	# Pull a list of trips for the week from the database.
	r = db.try0.find({"weeks":week})
	trip_list = list(r)
	logger.info("Pulled all trips from database")
	
	# Loop over all trips in the list and change them to server time (accounting for brazil time).
	original_length = len(trip_list)
	for server_time_trip in range(original_length):
		trip_list[server_time_trip]['timestamp']['hours'] = trip_list[server_time_trip]['timestamp']['hours'] + 3
		if trip_list[server_time_trip]['timestamp']['hours'] >= 24:
			trip_list[server_time_trip]['timestamp']['hours'] = trip_list[server_time_trip]['timestamp']['hours'] - 24
			trip_list[server_time_trip]['timestamp']['day'] = trip_list[server_time_trip]['timestamp']['day'] + 1
	
	#-----------------------------------------------------------------------#
	#						Function: Crawl 								#
	#-----------------------------------------------------------------------#
	def crawl(trip):
		# Call crawler and crawl trip.
		print ("Calling the Crawler")
		crawler.crawl_trip(trip)
		print ("Crawled Successfully")

	#-----------------------------------------------------------------------#
	#						Function: CSV Creator							#
	#-----------------------------------------------------------------------#
	def csv_creator(week,day):
		# Call csv_writer and create CSV.
		logger.info("Calling CSV Writer")
		csv_writer.make_csv(week,day)
		logger.info("CSV Written")

	#-----------------------------------------------------------------------#
	#						Function: Finish Scheduler						#
	#-----------------------------------------------------------------------#
	def finish_scheduler():
		# Return control to controller.
		logger.info("Bye. Stopping Scheduler.")
		global schd_bool
		schd_bool = 1

	# Schedule all instances for every trip.
	for item in range(len(trip_list)):
		s_time = str(trip_list[item]['timestamp']['hours'])+":"+str(trip_list[item]['timestamp']['minutes'])
		if trip_list[item]['timestamp']['day']==0:
			schedule.every().monday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day']==1:
			schedule.every().tuesday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day']==2:
			schedule.every().wednesday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day']==3:
			schedule.every().thursday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day']==4:
			schedule.every().friday.at(s_time).do(crawl,trip_list[item])
		elif trip_list[item]['timestamp']['day']==5:
			schedule.every().saturday.at(s_time).do(crawl,trip_list[item])

	# Schedule writing CSV at 3:01am from tuesday to saturday.
	schedule.every().tuesday.at("6:01").do(csv_creator,week,0)
	schedule.every().wednesday.at("6:01").do(csv_creator,week,1)
	schedule.every().thursday.at("6:01").do(csv_creator,week,2)
	schedule.every().friday.at("6:01").do(csv_creator,week,3)
	schedule.every().saturday.at("6:01").do(csv_creator,week,4)
	schedule.every().saturday.at("7:00").do(finish_scheduler)
	logger.info("Scheduled All Trips")

	# Send notification to Slack.
	url = "https://hooks.slack.com/services/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"
	schedule_trips_msg = "Sao Paulo 2012 Survey Normal-Crawler: Scheduling trips succesful."
	payload={"text": schedule_trips_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Normal-Crawler: Error while sending scheduler Slack notification.")
		logger.info(e)
		logger.info(schedule_trips_msg)

	# Loop till all trips crawled.
	while True and schd_bool==0:
		schedule.run_pending()
		time.sleep(1)
