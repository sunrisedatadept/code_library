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
from io import StringIO  

#CIVIS enviro variables
os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']
van_key = os.environ['VAN_PASSWORD']
strive_key = os.environ['STRIVE_PASSWORD']

# EA API credentials
username = 'brittany'
dbMode = '1'
password = van_key + '|' + dbMode
auth = HTTPBasicAuth(username, password)
headers = {"headers" : "application/json"}

# Initiate Redshift
rs = Redshift()

##### Set up logger #####
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')

##### SET TIME #####

max_time = datetime.now()
fifteen_minutes  = timedelta(minutes=15)
min_time = max_time - fifteen_minutes

max_time = max_time.strftime("%Y-%m-%dT%H:%M:%SZ")
min_time = min_time.strftime("%Y-%m-%dT%H:%M:%SZ")


##### REQUEST EXPORT JOB #####

logger.info('Starting Script!')
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

logger.info("Initiate Export Job")
response = requests.post(url, json = recent_contacts, headers = headers, auth = auth, stream = True)
jobId = str(response.json().get('exportJobId'))

##### GET EXPORT JOB ##### 

url = url + '/' + jobId
logger.info("Waiting for export")
timeout = 300   # [seconds]
timeout_start = time.time()

while time.time() < timeout_start + timeout:
	time.sleep(20) # twenty second delay
	try:
		response = requests.get(url, headers = headers, auth = auth)
		downloadLink = response.json().get('files')[0].get('downloadUrl')
		break
	except:
		logger.info("File not ready, trying again in 20 seconds")

##### CLEAN DATA #####
logger.info("Export Job Complete")

# Read in the data
downloadLink = response.json().get('files')[0].get('downloadUrl')
df = pd.read_csv(downloadLink)
logger.info(df.head())

# Remove contacts that were not created in the last 15 min
df['DateCreated']= pd.to_datetime(df['DateCreated'])
df_filtered = df.loc[df['DateCreated'].between(min_time, max_time)]

##### SEND TO STRIVE #####
# Opted in = 3, Unknown = 2
df_for_strive = df_filtered.loc[df_filtered['PhoneOptInStatus'] == 3]
df_for_strive = df_for_strive[["VanID", "FirstName", "LastName", "Phone"]]

url = "https://api.strivedigital.org/"
df_filtered.to_csv('data/sample_data.csv')

if len(df_for_strive) != 0:
	logger.info("Let's welcome these bad boys")
	logger.info("Logging new contacts in Redshift")
	new_contacts_table = Table.from_dataframe(df_filtered)
	# copy Table into Redshift, append new rows
	rs.copy(new_contacts_table, 'sunrise.new_contacts_summary' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)


else:
	logger.info("No new contacts in the last 15 minutes")
