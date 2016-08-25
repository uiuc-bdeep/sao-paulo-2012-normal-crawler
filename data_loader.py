'''
	File Name: data_loader.py
	Author: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Maintainer: Surya Teja Tadigadapa (tadigad2@illinois.edu)
	Description:	
		This script parses data from the CSV Trip Survey and creates a JSON file.
		A week number (string), city and survey year are added to the JSON Objects.
		A datestamp for every day of the week is also added.
		The JSON file is then uploaded to a MongoDB database.
'''

# Import libraries.
import os
import csv
import json
import random
import time
import datetime
import logging
import requests
from pymongo import MongoClient

#-----------------------------------------------------------------------#
#							Function: Load Data							#
#-----------------------------------------------------------------------#
def load_data(week_number):
	# Open Log and log date.
	logger = logging.getLogger("normal_crawler.data_loader")
	logger.info("Loading data for week: "+str(week_number))

	# Set up database connection.
	client = MongoClient(os.environ['DB_PORT_27017_TCP_ADDR'],27017)
	db = client.trial
	record = db.try0
	
	# Open CSV file, read headers and get length of data.
	dataFile = open('congestion_survey.csv')
	traffic_data_sheet = csv.reader(dataFile)
	headers = traffic_data_sheet.next()
	traffic_data_array = list(traffic_data_sheet)

	# Set keys to headers.
	keys = {}
	keys['ID_ORDEM'] = headers.index('ID_ORDEM')
	keys['TIPOVG'] = headers.index('TIPOVG')
	keys['H_SAIDA'] = headers.index('H_SAIDA')
	keys['MIN_SAIDA'] = headers.index('MIN_SAIDA')
	keys['DIA_SEM'] = headers.index('DIA_SEM')
	keys['Lat_O'] = headers.index('Lat_O')
	keys['Long_O'] = headers.index('Long_O')
	keys['Lat_D'] = headers.index('Lat_D')
	keys['Long_D'] = headers.index('Long_D')

	# Create list for JSON Objects.
	formatted_data = []

	# Create datestamps for trips.
	current_date = datetime.datetime.today().strftime('%Y/%m/%d')
	current_date_str = datetime.datetime.strptime(current_date, '%Y/%m/%d')
	week_date = current_date_str + datetime.timedelta(days=+1)
	monday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+2)
	tuesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+3)
	wednesday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+4)
	thursday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)
	week_date = current_date_str + datetime.timedelta(days=+5)
	friday = str(week_date.month)+"-"+str(week_date.day)+"-"+str(week_date.year)

	# Create a JSON Object for every trip in the CSV file.
	for i in range(len(traffic_data_array)):
		value = traffic_data_array[i]

		# Exclude trips without timestamps in the CSV file.
		if value[keys['MIN_SAIDA']] == '' or value[keys['DIA_SEM']] == '' or value[keys['H_SAIDA']] == '' or int(value[keys['DIA_SEM']]) == None or int(value[keys['DIA_SEM']]) == 0:
			continue

		# Set datestamp for each trip.
		if (int(value[keys['DIA_SEM']]) - 2)==0:
			datestamp = monday
		if (int(value[keys['DIA_SEM']]) - 2)==1:
			datestamp = tuesday
		if (int(value[keys['DIA_SEM']]) - 2)==2:
			datestamp = wednesday
		if (int(value[keys['DIA_SEM']]) - 2)==3:
			datestamp = thursday
		if (int(value[keys['DIA_SEM']]) - 2)==4:
			datestamp = friday

		# JSON Object for every trip.
		traffic_data_dict = {
			"trip_id": str(value[keys['ID_ORDEM']]),
			"survey":"2012",
			"city":"Sao Paulo",
			"weeks": week_number,
			# Week starts at day 2 (aka Monday == 2) in the CSV file, we start it at 0.
			"timestamp":
			{
				"hours": int(value[keys['H_SAIDA']]),
				"minutes": int(value[keys['MIN_SAIDA']]),
				"day": int(value[keys['DIA_SEM']]) - 2,
				"week": datestamp
			},
			"origin":
			{
				"latitude": value[keys['Lat_O']],
				"longitude": value[keys['Long_O']]
			},
			"destination":
			{
				"latitude": value[keys['Lat_D']],
				"longitude": value[keys['Long_D']]
			},
			"driving":
			{
				"distance": "-2",
				"time": "-2",
				"traffic": "-2"
			}
		}

		# Append every JSON trip into the list.
		formatted_data.append(traffic_data_dict)

	# Close the CSV file.
	dataFile.close()

	# Log data statistics.
	logger.info("Parsed CSV file and created JSON Objects")
	logger.info("Number of trips for this week: "+str(len(formatted_data)))

	# Write all JSON Objects to a JSON file.
	json_all_trips = json.dumps(formatted_data, sort_keys = True, indent = 4, separators = (',',':'))
	json_file = open('normal_crawler_all_trips.json', 'w')  
	json_file.write(json_all_trips)
	json_file.close()
	logger.info("Created JSON file.")

	# Push JSON Objects from the file into the database.
	json_file = open("normal_crawler_all_trips.json", 'r')
	json_trips = json.loads(json_file.read())
	for item in json_trips:
		record.insert(item)
	json_file.close()
	logger.info("Loaded data into the database.")

	# Send notification to Slack.
	url = "https://hooks.slack.com/services/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"
	data_loader_msg = "Sao Paulo 2012 Survey Normal-Crawler: Data loading succesful."
	payload={"text": data_loader_msg}
	try:
		r = requests.post(url, data=json.dumps(payload))
	except requests.exceptions.RequestException as e:
		logger.info("Sao Paulo 2012 Survey Normal-Crawler: Error while sending data loader Slack notification.")
		logger.info(e)
		logger.info(data_loader_msg)
