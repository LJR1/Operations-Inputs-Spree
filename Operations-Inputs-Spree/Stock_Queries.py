# -*- coding: utf-8 -*-
"""
Created on Fri Mar 27 10:59:09 2015

@author: Laurie.Richardson
"""

import pandas as pd
from pandas import DataFrame
import gspread
import numpy as np
#import time
from datetime import date, timedelta
from pandas import ExcelWriter
from openpyxl.reader.excel import load_workbook

#Email Import Library
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import Encoders

today=date.today()

Queries_File=raw_input("Please provide your file name:")

#Create rolling stock dataframe

Queries=pd.read_csv(Queries_File,index_col=None, na_values=['NA'])
QueriesSummary=Queries[['Status','Category','Requester','Request date','Solved date']]
QueriesSummary['OpenQueries']=1
QueriesSummary['Request date']=pd.to_datetime(QueriesSummary['Request date'],coerce=True)
QueriesSummary['Solved date']=pd.to_datetime(QueriesSummary['Solved date'],coerce=True)
OpenQueriesByBuyer=QueriesSummary.loc[(QueriesSummary['Status']=='Pending') | (QueriesSummary['Status']=='Open')]

OpenQueriesByBuyer['Day of SLA Expiry']=OpenQueriesByBuyer['Request date']+timedelta(7)
OpenQueriesByBuyer['Expired']=OpenQueriesByBuyer['Day of SLA Expiry']<date.today()

#OpenQueriesByBuyer

OpenQueriesSumm=pd.pivot_table(OpenQueriesByBuyer,values='OpenQueries',index=['Requester'],aggfunc=np.sum)
OpenQueriesDF=DataFrame(OpenQueriesSumm).sort(columns='OpenQueries',ascending=False)

#OpenQueriesByBuyerOlderThan7Days

OpenQueriesSumm_7_Days=pd.pivot_table(OpenQueriesByBuyer,values='OpenQueries',index=['Expired','Requester'],aggfunc=np.sum)
OpenQueriesSumm_7_Days_DF=DataFrame(OpenQueriesSumm_7_Days)








#QueryQuantityByBuyerOverTime
#TypeofQuery
