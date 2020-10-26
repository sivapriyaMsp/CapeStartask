# -*- coding: utf-8 -*-
"""DataPipelineCode.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/12fYrYecMY43PhatuwGlNZB1GII4TaQuN
"""

from google.colab import auth
auth.authenticate_user()
#Afer authendication we can read file from google drive

from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from oauth2client.client import GoogleCredentials
gauth = GoogleAuth()
gauth.credentials = GoogleCredentials.get_application_default()
drive = GoogleDrive(gauth)

#get file from Google drive location with file id which share through he mail
myfile = drive.CreateFile({'id': '1ic-jC8EliZl8OKtVrtACgj02ncA0JUjf'})
myfile.GetContentFile('U.S._Chronic_Disease_Indicators__CDI_.csv')

#from pyspark.sql import SQLContext
import pandas as pd
frm = pd.read_csv('U.S._Chronic_Disease_Indicators__CDI_.csv', header=None)
print(frm)
#Store the daaframe data into local directory
frm.to_csv('data_into_csv.csv')


