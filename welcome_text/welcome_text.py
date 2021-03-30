# load the necessary packages
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
import time
from parsons import Redshift, Table, VAN, S3, utilities
from datetime import date, datetime
from requests.exceptions import HTTPError
import os
import logging
import json 
import time
from urllib.parse import urljoin
import urllib.request
from datetime import datetime, timedelta


#Local enviro variables
os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME']
os.environ['REDSHIFT_PASSWORD']
os.environ['S3_TEMP_BUCKET']
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']
van_key = os.environ['VAN_API_KEY']

# Initiate Redshift
rs = Redshift()

# EA API credentials
username = 'brittany'
dbMode = '1'
password = van_key + '|' + dbMode
auth = HTTPBasicAuth(username, password)
headers = {"headers" : "application/json"}

## EA endpoint url
base_url = 'https://api.securevan.com/v4/'

########################## SET TIME ###############################

max_time = datetime.now()
fifteen_minutes  = timedelta(minutes=15)
min_time = max_time - fifteen_minutes

max_time = max_time.strftime("%Y-%m-%dT%H:%M:%SZ")
min_time = min_time.strftime("%Y-%m-%dT%H:%M:%SZ")


###################### REQUEST EXPORT JOB ###################################

print("Starting Script")

job = "changedEntityExportJobs"
url = urljoin(base_url, job)

recent_contacts = {
  "dateChangedFrom": 	min_time,
  "dateChangedTo" : 	max_time,
  "resourceType": 		"Contacts",
  "requestedFields": 	["VanID", "FirstName", "LastName", "Phone", "PhoneOptInStatus", "DateCreated" ]
}

###################### REQUEST EXPORT JOB ###################################

print("Initiate Export Job")
response = requests.post(url, json = recent_contacts, headers = headers, auth = auth, stream = True)
jobId = str(response.json().get('exportJobId'))

###################### GET EXPORT JOB ################################### 

url = url + '/' + jobId
response = requests.get(url, headers = headers, auth = auth)
print(response.text)
print("Waiting for export")
print(response.status_code)
while response.status_code != 201:
	time.sleep(20) # twenty second delay
	try:
		response = requests.get(url, headers = headers, auth = auth)
		downloadLink = response.json().get('files')[0].get('downloadUrl')
		urllib.request.urlretrieve(downloadLink, 'data/contacts.csv')
		break
	except:
		print ("File not ready, trying again in 20 seconds")

################### SEND TO STRIVE ################################
print("Export Job Complete")
df = pd.read_csv('data/contacts.csv')
print(df.head())

