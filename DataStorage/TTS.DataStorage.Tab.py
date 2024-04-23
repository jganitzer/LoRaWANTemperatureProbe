#!/usr/bin/python3

theApplication = ""
theAPIKey = ""
theRegion = "EU1"

# How many records you want, set to 0 if you want everything available
theNumberOfRecords = 0

# From which date you want the records returned. Note, DS only holds 7 days of data
theSinceDateTime = "2021-05-16T19:00:00Z" # Format is 2020-10-01T12:13:14Z

VER  = "2021-04-27 v1.1"
import os, sys, logging, time
print(os.path.basename(__file__) + " " + VER)

print("Imports:")
import requests
import json
import csv
from datetime import datetime
import numpy as np
import pandas as pd
import xlsxwriter

def make_graph(pathNFile):

	# Get our input data
	temperature_data = pd.read_csv(pathNFile, delimiter=";")
	print(temperature_data.head())

	# Initialize the excel output file
	excel_file_path = './Temperaturgrafik.xlsx'
	workbook = xlsxwriter.Workbook(excel_file_path)
	temperature_worksheet = workbook.add_worksheet()
	date_format = workbook.add_format({'num_format': 'dd/mm/yy'})

	chart = workbook.add_chart({'type': 'scatter', 'subtype': 'smooth_with_markers'})
	peak = 0
	peak_min = 0
	for i, col_name in enumerate(temperature_data.columns):
		temperature_worksheet.write(0, i, col_name)
		if (i == 0):
			temperature_worksheet.write_column(1, i, temperature_data[col_name], date_format)
		else:
			temperature_worksheet.write_column(1, i, temperature_data[col_name])

			col_letter = xlsxwriter.utility.xl_col_to_name(i)

			chart.add_series({'categories': '=Sheet1!$A$2:$A$' + str(2 + len(temperature_data['Temp1'] - 1)),
							  'values': '=Sheet1!$' + col_letter + '$2:$' + col_letter + '$755',
							  'name': col_name})
			if (np.max(temperature_data[col_name]) > peak):
				peak = np.max(temperature_data[col_name])
				peak_index = np.argmax(temperature_data[col_name])

			if (np.min(temperature_data[col_name]) < peak_min):
				peak_min = np.min(temperature_data[col_name])
				peak_min_index = np.argmin(temperature_data[col_name])

	chart.set_size({'width': 2000, 'height': 1000})
	chart.set_x_axis({'name': 'Zeipunkt'})
	chart.set_title({'name': 'Messlanze'})
	chart.set_y_axis({'name': 'Temperatur', 'min': peak_min, 'max': 1.2 * peak})

	temperature_worksheet.insert_chart('A1', chart)
	workbook.close()

theURL  = "https://" + theRegion.lower() + ".cloud.thethings.network/api/v3/as/applications/"
theURL += theApplication + "/packages/storage/uplink_message?order=received_at&type=uplink_message"

if theNumberOfRecords:
	theURL += "&limit=" + str(theNumberOfRecords)

if theSinceDateTime:
	theURL += "&after=" + theSinceDateTime

# These are the headers required in the documentation.
theHeaders = { 'Accept': 'text/event-stream', 'Authorization': 'Bearer ' + theAPIKey }

print("\n\nFetching from data storage  ...\n")

r = requests.get(theURL, headers=theHeaders)

print("URL: " + r.url)
print("Status: " + str(r.status_code))
print()


# The text returned is one block of JSON per uplink with a blank line between.
# Event Stream (see headers above) is a connection type that sends a message when it 
# becomes available. This script is about downloading a bunch of records in one go
# So we have to turn the response in to an array and remove the blank lines.

theJSON = "{\"data\": [" + r.text.replace("\n\n", ",")[:-1] + "]}";

someJSON = json.loads(theJSON)
#print(json.dumps(someJSON, indent=4))
someUplinks = someJSON["data"]

#print(json.dumps(someJSON, indent=4))	# Uncomment this to fill your terminal screen with JSON

# Output to timestamped file
now = datetime.now()
#pathNFile = "DataStorage-" + now.strftime("%Y%m%d%H%M%S") + ".csv"

def dataToFile(runCounter, runCounter2):

	pathNFileAll = os.path.dirname(os.path.realpath(__file__)) + "/DataStorage-ALL" + ".txt"

	if (not os.path.isfile(pathNFileAll)):
		with open(pathNFileAll, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile,  delimiter=";")
			fw.writerow(["Timestamp", "Temp1", "Temp2", "Temp3", "Temp4", "Temp5", "Temp6"])

	try:
		StorageDfAll= pd.read_csv(pathNFileAll, sep=';')

	except: 
		print("Files could not be opened")
		runCounter += 1
		if (runCounter <= 10):
			print("Try Again in 60 Seconds")
			time.sleep(60)
			dataToFile(runCounter, runCounter2)
		else: sys.exit(0)

	for anUplink in someUplinks:
		someJSON = anUplink["result"]

		received_at = someJSON["received_at"]

		temp1 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp1'])).replace(".", ",")
		temp2 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp2'])).replace(".", ",")
		temp3 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp3'])).replace(".", ",")
		temp4 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp4'])).replace(".", ",")
		temp5 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp5'])).replace(".", ",")
		temp6 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp6'])).replace(".", ",")

		temp_data = {'Timestamp': [received_at], 'Temp1': [temp1], 'Temp2':[temp2], 'Temp3': [temp3], 'Temp4': [temp4], 'Temp5': [temp5], 'Temp6': [temp6]}
		df_temp = pd.DataFrame(data=temp_data)

		StorageDfAll = pd.concat([StorageDfAll, df_temp],  ignore_index=True)

	StorageDfAll = StorageDfAll.drop_duplicates(subset=['Timestamp'])

	try:
		StorageDfAll.to_csv(pathNFileAll, sep=';', index=False)

	except: 
		print("Failed to write to file")
		runCounter2 += 1
		if (runCounter2 <= 10):
			print("Try Again in 60 Sekonds")
			time.sleep(60)
			dataToFile(runCounter, runCounter2)
		else: sys.exit(0)

runCounter = 0
runCounter2 = 0

dataToFile(runCounter, runCounter2)

