# load the necessary packages
import pandas as pd
import numpy as np
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

#########################################################################

def download_file(response):
	downloadLink = response.json().get('files')[0].get('downloadUrl')
	return(urllib.request.urlretrieve(downloadLink, 'data/contacts.csv'))


###################### REQUEST EXPORT JOB ###################################

print("Starting Script")

job = "changedEntityExportJobs"
url = urljoin(base_url, job)

recent_contacts = {
  "dateChangedFrom": 	"2021-03-10T01:02:03+04:00",
  "dateChangedTo" : 	"2021-03-20T01:02:03+04:00",
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
while response.json().get('jobStatus') == 'Pending':
    time.sleep(20) # twenty second delay
    try:
    	download_file(response)
    	break
    except:
    	print ("File not ready, trying again in 20 seconds")

################### SEND TO STRIVE ################################
print("Export Job Complete")
df = pd.read_csv('data/contacts.csv')
print(df.head())
