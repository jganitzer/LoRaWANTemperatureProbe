#!/usr/bin/python3

User = ""
Password = ""
theRegion = "EU1"

VER  = "2021-05-24 v1.2"
from gettext import install
import os, sys, logging, time
print(os.path.basename(__file__) + " " + VER)

print("Imports:")

import paho.mqtt.client as mqtt
import json
import csv
from datetime import datetime
import numpy as np
import pandas as pd
import xlsxwriter

print("Functions:")

# Write uplink to tab file
def saveToFile(someJSON):
	end_device_ids = someJSON["end_device_ids"]
	device_id = end_device_ids["device_id"]
	application_id = end_device_ids["application_ids"]["application_id"]
	
	received_at = someJSON["received_at"]
	
	uplink_message = someJSON["uplink_message"];
	f_port = uplink_message["f_port"];
	f_cnt = uplink_message["f_cnt"];
	frm_payload = uplink_message["frm_payload"];
	rssi = uplink_message["rx_metadata"][0]["rssi"];
	snr = uplink_message["rx_metadata"][0]["snr"];
	data_rate_index = uplink_message["settings"]["data_rate_index"];
	consumed_airtime = uplink_message["consumed_airtime"];	
	
	# Daily log of uplinks
	now = datetime.now()
	pathNFile = now.strftime("%Y%m%d") + ".txt"
	print(pathNFile)
	if (not os.path.isfile(pathNFile)):
		with open(pathNFile, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile, dialect='excel-tab')
			fw.writerow(["received_at", "application_id", "device_id", "f_port", "f_cnt", "frm_payload", "rssi", "snr", "data_rate_index", "consumed_airtime"])
	
	with open(pathNFile, 'a', newline='') as tabFile:
		fw = csv.writer(tabFile, dialect='excel-tab')
		fw.writerow([received_at, application_id, device_id, f_port, f_cnt, frm_payload, rssi, snr, data_rate_index, consumed_airtime])

	# Application log
	pathNFile = application_id + ".txt"
	print(pathNFile)
	if (not os.path.isfile(pathNFile)):
		with open(pathNFile, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile, dialect='excel-tab')
			fw.writerow(["received_at", "device_id", "f_port", "f_cnt", "frm_payload", "rssi", "snr", "data_rate_index", "consumed_airtime"])
	
	with open(pathNFile, 'a', newline='') as tabFile:
		fw = csv.writer(tabFile, dialect='excel-tab')
		fw.writerow([received_at, device_id, f_port, f_cnt, frm_payload, rssi, snr, data_rate_index, consumed_airtime])
	
	# Device log
	pathNFile = application_id + "__" + device_id + ".txt"
	print(pathNFile)
	if (not os.path.isfile(pathNFile)):
		with open(pathNFile, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile, dialect='excel-tab')
			fw.writerow(["received_at", "f_port", "f_cnt", "frm_payload", "rssi", "snr", "data_rate_index", "consumed_airtime"])
	
	with open(pathNFile, 'a', newline='') as tabFile:
		fw = csv.writer(tabFile, dialect='excel-tab')
		fw.writerow([received_at, f_port, f_cnt, frm_payload, rssi, snr, data_rate_index, consumed_airtime])


# MQTT event functions
def on_connect(mqttc, obj, flags, rc):
    print("\nConnect: rc = " + str(rc))

def on_message(mqttc, obj, msg):
	print("\nMessage: " + msg.topic + " " + str(msg.qos)) # + " " + str(msg.payload))
	parsedJSON = json.loads(msg.payload)
	to_file(parsedJSON)

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("\nSubscribe: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
    print("\nLog: "+ string)
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, string)

def to_file(someJSON):
	# Output to timestamped file

	pathNFile = os.path.dirname(os.path.realpath(__file__)) +  "/Temperaturdaten-MQTT" + ".txt"
	print(pathNFile)
	if (not os.path.isfile(pathNFile)):
		with open(pathNFile, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile, delimiter=";")
			fw.writerow(["Timestamp", "Temp1", "Temp2", "Temp3", "Temp4", "Temp5", "Temp6"])

	received_at = someJSON["received_at"]

	temp1 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp1'])).replace(".", ",")
	temp2 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp2'])).replace(".", ",")
	temp3 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp3'])).replace(".", ",")
	temp4 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp4'])).replace(".", ",")
	temp5 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp5'])).replace(".", ",")
	temp6 = ("{:.2f}".format(someJSON['uplink_message']['decoded_payload']['temp6'])).replace(".", ",")

	try:
		with open(pathNFile, 'a', newline='') as tabFile:
			fw = csv.writer(tabFile, delimiter=";")
			fw.writerow([received_at, temp1, temp2, temp3, temp4, temp5, temp6])

	except Exception:
		print("Not able to write to file")

def make_graph(pathNFile):
	temperature_data = pd.read_csv(pathNFile, delimiter=";")
	print(temperature_data.head())

	# Initialize the excel output file
	excel_file_path =  './Temperaturgrafik.xlsx'
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

def start_script(runCounter):
	try: 
		mqttc.loop(10) 	# seconds timeout / blocking time
		print(".", end="", flush=True)	# feedback to the user that something is actually happening
	except Exception:
		print("Something crashed the program. Let's restart it")
		runCounter += 1
		if (runCounter <= 30):
			mqttc.connect(theRegion.lower() + ".cloud.thethings.network", 8883, 60)
			print("Subscribe")
			mqttc.subscribe("#", 0)	# all device uplinks

			print("Try Again and wait 30 seconds")
			time.sleep(30)
			start_script(runCounter)
		else: sys.exit(0)


print("Init mqtt client")
mqttc = mqtt.Client()

print("Assign callbacks")
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
mqttc.on_message = on_message

print("Connect")
# Setup authentication from settings above
mqttc.username_pw_set(User, Password)

certpath = os.path.dirname(os.path.realpath(__file__)) + "/mqtt-ca.pem"
mqttc.tls_set(ca_certs= "mqtt-ca.pem") # Use this if you get security errors
# It loads the TTI security certificate. Download it from their website from this page: 
# https://www.thethingsnetwork.org/docs/applications/mqtt/api/index.html

mqttc.connect(theRegion.lower() + ".cloud.thethings.network", 8883, 60)

print("Subscribe")
mqttc.subscribe("#", 0)	# all device uplinks

run = True

print("Body of program:")

print("And run forever")

while run:
	runCounter = 0
	start_script(runCounter)





