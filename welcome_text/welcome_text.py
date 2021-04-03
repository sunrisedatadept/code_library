# load the necessary packages
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import time
from parsons import Redshift, Table, VAN, S3, utilities
from datetime import date, datetime
from requests.exceptions import HTTPError
import os
import json 
import time
from urllib.parse import urljoin
import urllib.request
from datetime import datetime, timedelta
import sys

#Local enviro variables
van_key = os.environ['VAN_API_KEY']
strive_key = os.environ['STRIVE_KEY']
campaign_id = os.environ['STRIVE_CAMPAIGN_ID']

# EA API credentials
username = 'welcometext'
dbMode = '1'
password = van_key + '|' + dbMode
auth = HTTPBasicAuth(username, password)
headers = {"headers" : "application/json"}

##### SET TIME #####

max_time = datetime.now()
fifteen_minutes  = timedelta(minutes=15)
min_time = max_time - fifteen_minutes

max_time = max_time.strftime("%Y-%m-%dT%H:%M:%SZ")
min_time = min_time.strftime("%Y-%m-%dT%H:%M:%SZ")


##### REQUEST EXPORT JOB #####
base_url = 'https://api.securevan.com/v4/'
job = "changedEntityExportJobs"
url = urljoin(base_url, job)

recent_contacts = {
  "dateChangedFrom": 	min_time,
  "dateChangedTo" : 	max_time,
  "resourceType": 		"Contacts",
  "requestedFields": 	["VanID", "FirstName", "LastName", "Phone", "PhoneOptInStatus", "DateCreated" ]
}

##### REQUEST EXPORT JOB #####

print("Initiate Export Job")
response = requests.post(url, json = recent_contacts, headers = headers, auth = auth, stream = True)
jobId = str(response.json().get('exportJobId'))

##### GET EXPORT JOB ##### 

url = url + '/' + jobId
print("Waiting for export")
timeout = 1000   # [seconds]
timeout_start = time.time()

while time.time() < timeout_start + timeout:
	time.sleep(20) # twenty second delay
	try:
		response = requests.get(url, headers = headers, auth = auth)
		downloadLink = response.json().get('files')[0].get('downloadUrl')
		break
	except:
		print("File not ready, trying again in 20 seconds")

if time.time() == timeout_start + timeout:
	sys.exit("Export Job failed to download!")
else:
	print("Export Job Complete")
##### CLEAN DATA #####

# Read in the data
df = pd.read_csv(downloadLink)
print(df.head())
print(df['DateCreated'])


# Filter for contacts that were created today
# EveryAction returns a date, not a datetime, for DateCreated
# Relying on Strive's dedupe upsert logic to not text people twice
df['DateCreated']= pd.to_datetime(df['DateCreated'])
df_filtered = df.loc[df['DateCreated'] ==  datetime.now().date()]

# Filter for contacts that have opted in. Opted in = 3
df_for_strive = df_filtered.loc[df_filtered['PhoneOptInStatus'] == 3]
df_for_strive = df_for_strive[["VanID", "FirstName", "LastName", "Phone"]]

##### SEND TO STRIVE #####

url = "https://api.strivedigital.org/"
df.to_csv('data/sample_data.csv')

headers = {
  'Content-Type': 'application/json',
  'Authorization': 'Bearer ' + strive_key
}

if len(df_for_strive) != 0:
	print("New folk to welcome! Let's send to Strive. They'll handle any deduping.")
	
	for person in len(df_for_strive):
			phone_number = df_for_strive['Phone']
			first_name = df_for_strive['FirstName']
			last_name = df_for_strive['LastName']
			payload = {
				    "phone_number": phone_number,
				    "campaign_id": campaign_id,
				    "first_name": first_name,
				    "last_name": last_name,
				    "opt_in": True
				}

			response = requests.request("POST", 'https://api.strivedigital.org/members', headers=headers, data=json.dumps(payload))
			if response.status_code == 201:
				print(f"Successfully added, {first_name}, {last_name}")
			else:
				print(f"Error, {response.status_code}")
	
else:
	print("No contacts to send to Strive.")





