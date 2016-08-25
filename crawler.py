'''
	File Name: crawler.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script parses data from the JSON Object to form a URL and then requests
		data from the Google API. The response data is updated in the corresponding 
		trip in the database.

	API Keys:
		Peter's Key Extended Crawler = AIzaSyADEXdHuYJDPa2K5oSzBxAUxCGEzzRvzi0
		Peter's Key Normal Crawler = AIzaSyCkhLse_YCh-hiOltq8MQiDvuRxtyMEbNw
		Surya's Key = AIzaSyAjcwpLGX5VExXKEpq63WCWzv3ibkJGPKY
'''

# Import libraries.
import os
import json
import requests
import datetime
import logging
from pymongo import MongoClient
from bson.objectid import ObjectId

#-----------------------------------------------------------------------#
#							Function: Crawl Trip						#
#-----------------------------------------------------------------------#
def crawl_trip(jsonObj):
	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial

	# Open Log. Parse and log trip ID.
	logger = logging.getLogger("normal_crawler.crawler")
	t_id = jsonObj['trip_id']
	logger.info("Crawling Trip: "+str(t_id))

	# Parse other parameters from the trip.
	db_id = jsonObj['_id']
	lat_o = jsonObj['origin']['latitude']
	lon_o = jsonObj['origin']['longitude']
	lat_d = jsonObj['destination']['latitude']
	lon_d = jsonObj['destination']['longitude']
	t_week = jsonObj['weeks']
	t_day = jsonObj['timestamp']['day']
	t_type = "driving"
	t_mode = "driving"
	
	# Create URL.
	base_url = "https://maps.googleapis.com/maps/api/distancematrix/json?"
	final_url = base_url+"origins="+lat_o+","+lon_o+"&destinations="+lat_d+","+lon_d+"&mode="+t_mode+"&departure_time=now&key=AIzaSyCkhLse_YCh-hiOltq8MQiDvuRxtyMEbNw"

	# Query Google API.
	try:
		r = requests.get(final_url)
		logger.info("Queried API for trip: "+str(t_id))

		# Print to terminal, so can check response in docker logs.
		print r.content

		# Convert response to JSON.
		response = json.loads(r.content)

		# Set primary response to -3.
		t_traffic = "-3"
		t_dist = "-3"
		t_time = "-3"

		# Check if valid response.
		if response["status"] == "OK":
			for it1 in response['rows'][0]['elements'][0].get('distance',[]):
				t_dist = str(response['rows'][0]['elements'][0]['distance']['value'])
			for it2 in response['rows'][0]['elements'][0].get('duration',[]):
				t_time = str(response['rows'][0]['elements'][0]['duration']['value'])
			for it3 in response['rows'][0]['elements'][0].get('duration_in_traffic',[]):
				t_traffic = str(response['rows'][0]['elements'][0]['duration_in_traffic']['value'])
			logger.info("Status is OK.")
		else:
			logger.info("Status is not okay. It is: "+str(response["status"]))

	# If an exception is generated.
	except requests.exceptions.RequestException as e:
		logger.info("Error while crawling trip: "+str(t_id))
		logger.info(str(e))
		t_traffic = "-1"
		t_dist = "-1"
		t_time = "-1"

	# Update database
	db.try0.update({"_id" : db_id}, {"$set": {t_type: {"distance" : t_dist , "time" : t_time , "traffic" : t_traffic }}})
	logger.info("Modified Database for trip: "+str(t_id))
