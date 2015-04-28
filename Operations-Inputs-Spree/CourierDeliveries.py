# -*- coding: utf-8 -*-
"""
Created on Tue Nov 25 09:31:09 2014

"""

import pandas as pd
from pandas import DataFrame
import gspread
import numpy as np
from datetime import datetime, timedelta, date
from pandas.tseries.offsets import *
from pandas import ExcelWriter
import re
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
pd.options.mode.chained_assignment = None  # default='warn'

Csv_File_Info = raw_input("Please enter the name and location of your Sales CSV file:")

df=pd.read_csv(Csv_File_Info,na_values=['NA'])

df['DATE MBI0001']=pd.to_datetime(df['DATE MBI0001'],coerce=True)
today = date.today()
df1=df[df['DATE MBI0001']>date.today()-timedelta(7)]
x=df1.set_index(pd.DatetimeIndex(df1['DATE MBI0001']))
x.index.names = ['DateOfOrder']
TimeOutput=x

#1) Order Not Created Info
OrderNotCreated=TimeOutput[TimeOutput['Order created :  OBOI0001']=='No']

OrderOutput=OrderNotCreated[[
'Order number',
'Order created :  OBOI0001',
'Order packed: OBOI1001',
'Address error OBOCOO9',
'Parcel at dispatch area TRKI0001 status 3 ',
'Parcel at handed over to courier  TRKI0001 status 7',
'Parcel delivered TRKI0001 status 9',
'OK to Ship sent ', 
'Not Ok to ship sent',
'Fraud list',
'Lost order ',
]]


#2) Measured Time between Order Created (OBO0001) and Order Packed (OBOI1001) 

PackedTime=TimeOutput[(TimeOutput['Order created :  OBOI0001']=='Yes')] 
PackedFilter=PackedTime[(PackedTime['Order packed: OBOI1001']=='Yes')]

PackedOutput=PackedFilter[[
'Order number',
'Order created :  OBOI0001',
'DATE Order OBOI0001',	
'Order packed: OBOI1001',	
'Date Order OBOI1001',
'Lost order ',
]]

PackedOutput['DATE Order OBOI0001']=pd.to_datetime(PackedOutput['DATE Order OBOI0001'],coerce=True).fillna(1000)
PackedOutput['Date Order OBOI1001']=pd.to_datetime(PackedOutput['Date Order OBOI1001'],coerce=True).fillna(1000)

A = [c.date() for c in PackedOutput['DATE Order OBOI0001']]
B = [d.date() for d in PackedOutput['Date Order OBOI1001']]

PackedOutput['PickPackTime (Days)'] = np.busday_count(A, B)
PackedSorted=PackedOutput.sort(['PickPackTime (Days)'],ascending=False)


# 3) Orders with Address Error OBOCOO9

AddressError=TimeOutput[(TimeOutput['Address error OBOCOO9']=='Yes')]

AddressErrorOutput=AddressError[[
'Order number',
'Order created :  OBOI0001',
'DATE Order OBOI0001',	
'Order packed: OBOI1001',	
'Date Order OBOI1001',
'Address error OBOCOO9',
'Date Address error OBOCOO9'
]]

# 4) Time between Order Packed (OBOI1001) and Parcel at dispatch area TRKI0001 status 3

WaitingTime=TimeOutput[(TimeOutput['Order packed: OBOI1001']=='Yes')] 
WaitingFilter=WaitingTime[(WaitingTime['Parcel at dispatch area TRKI0001 status 3 ']=='Yes')]

WaitingOutput=WaitingFilter[[
'Order number',
'Order packed: OBOI1001',	
'Date Order OBOI1001',
'Parcel at dispatch area TRKI0001 status 3 ',
'Date Parcel at dispatch area TRKI0001 status 3',
'OK to Ship sent ',
'Date OK to Ship sent ' 
]]

WaitingOutput['Date Order OBOI1001']=pd.to_datetime(WaitingOutput['Date Order OBOI1001'],coerce=True).fillna(1000)
WaitingOutput['Date Parcel at dispatch area TRKI0001 status 3']=pd.to_datetime(WaitingOutput['Date Parcel at dispatch area TRKI0001 status 3'],coerce=True).fillna(1000)


E = [g.date() for g in WaitingOutput['Date Order OBOI1001']]
F = [h.date() for h in WaitingOutput['Date Parcel at dispatch area TRKI0001 status 3']]
WaitingOutput['WaitingTime (Days)'] = np.busday_count(E, F)
WaitingSorted=WaitingOutput.sort(['WaitingTime (Days)'],ascending=False)


#5) Time between Parcel at dispatch area TRKI0001 status 3 to Parcel at handed over to courier TRKI0001 status 7

HandOverTime=TimeOutput[(TimeOutput['Parcel at dispatch area TRKI0001 status 3 ']=='Yes')] 
HandOverFilter=HandOverTime[(HandOverTime['Parcel at handed over to courier  TRKI0001 status 7']=='Yes')]

HandOverOutput=HandOverFilter[[
'Order number',
'Parcel at dispatch area TRKI0001 status 3 ',
'Date Parcel at dispatch area TRKI0001 status 3',
'Parcel at handed over to courier  TRKI0001 status 7',
'Date Parcel at handed over to courier  TRKI0001 status 7',
'OK to Ship sent ',
'Date OK to Ship sent ' 
]]

HandOverOutput['Date Parcel at dispatch area TRKI0001 status 3']=pd.to_datetime(HandOverOutput['Date Parcel at dispatch area TRKI0001 status 3'],coerce=True).fillna(1000)
HandOverOutput['Date Parcel at handed over to courier  TRKI0001 status 7']=pd.to_datetime(HandOverOutput['Date Parcel at handed over to courier  TRKI0001 status 7'],coerce=True).fillna(1000)

I = [k.date() for k in HandOverOutput['Date Parcel at dispatch area TRKI0001 status 3']]
J = [l.date() for l in HandOverOutput['Date Parcel at handed over to courier  TRKI0001 status 7']]
HandOverOutput['HandOverTime (Days)'] = np.busday_count(I, J)
HandOverSorted=HandOverOutput.sort(['HandOverTime (Days)'],ascending=False)

# 6) Parcel handed over to courier  TRKI0001 status 7 to Parcel delivered TRKI0001 status 9

DeliveryTime=TimeOutput[(TimeOutput['Parcel at handed over to courier  TRKI0001 status 7']=='Yes')] 
DeliveryFilter=DeliveryTime[(DeliveryTime['Parcel delivered TRKI0001 status 9']=='Yes')]

DeliveryOutput=DeliveryFilter[[
'Order number',
'Parcel at handed over to courier  TRKI0001 status 7',
'Date Parcel at handed over to courier  TRKI0001 status 7',
'Parcel delivered TRKI0001 status 9',
'Date Parcel delivered TRKI0001 status 9',
'OK to Ship sent ',
'Date OK to Ship sent ' 
]]

DeliveryOutput['Date Parcel at handed over to courier  TRKI0001 status 7']=pd.to_datetime(DeliveryOutput['Date Parcel at handed over to courier  TRKI0001 status 7'],coerce=True).fillna(1000)
DeliveryOutput['Date Parcel delivered TRKI0001 status 9']=pd.to_datetime(DeliveryOutput['Date Parcel delivered TRKI0001 status 9'],coerce=True).fillna(1000)

M = [o.date() for o in DeliveryOutput['Date Parcel at handed over to courier  TRKI0001 status 7']]
N = [p.date() for p in DeliveryOutput['Date Parcel delivered TRKI0001 status 9']]
DeliveryOutput['DeliveryTime (Days)'] = np.busday_count(M, N)
DeliverySorted=DeliveryOutput.sort(['DeliveryTime (Days)'],ascending=False)

# 7) Delivery OK to Ship (N)

OrderNotOK=TimeOutput[TimeOutput['OK to Ship sent ']=='No']

OrderNotOKOutput=OrderNotOK[[
'Order number',
'OK to Ship sent ', 
'Date OK to Ship sent ', 
'Order created :  OBOI0001',
'Order packed: OBOI1001',
'Address error OBOCOO9',
'Parcel at dispatch area TRKI0001 status 3 ',
'Parcel at handed over to courier  TRKI0001 status 7',
'Parcel delivered TRKI0001 status 9',
]]


#Create Excel output file
today = date.today()
writer = ExcelWriter('ExceptionOutput' + str(today) + '.xlsx')
OrderOutput.to_excel(writer,'OrderNotCreated')
PackedSorted.to_excel(writer,'OrderToPacked')
AddressErrorOutput.to_excel(writer,'AddressErrorOutput')
WaitingSorted.to_excel(writer,'PackedToDispatch')
HandOverSorted.to_excel(writer,'DispatchToCourier')
DeliverySorted.to_excel(writer,'CourierToDelivered')
OrderNotOKOutput.to_excel(writer,'NotOKToSHip')

#Format excel doc (Create a for loop going forward)
workbook = writer.book

worksheet1 = writer.sheets['OrderNotCreated']
worksheet2 = writer.sheets['OrderToPacked']
worksheet3 = writer.sheets['AddressErrorOutput']
worksheet4 = writer.sheets['PackedToDispatch']
worksheet5 = writer.sheets['DispatchToCourier']
worksheet6 = writer.sheets['CourierToDelivered']
worksheet7 = writer.sheets['NotOKToSHip']

worksheet1.set_column('A:A', 30)
worksheet1.set_column('B:B', 30)
worksheet1.set_column('C:C', 30)
worksheet1.set_column('D:D', 30)
worksheet1.set_column('E:E', 30)
worksheet1.set_column('F:F', 30)
worksheet1.set_column('G:G', 30)
worksheet1.set_column('H:H', 30)
worksheet1.set_column('I:I', 30)   
worksheet1.set_column('J:J', 30)

worksheet2.set_column('A:A', 30)
worksheet2.set_column('B:B', 30)
worksheet2.set_column('C:C', 30)
worksheet2.set_column('D:D', 30)
worksheet2.set_column('E:E', 30)
worksheet2.set_column('F:F', 30)
worksheet2.set_column('G:G', 30)
worksheet2.set_column('H:H', 30)
worksheet2.set_column('I:I', 30)   
worksheet2.set_column('J:J', 30)

worksheet3.set_column('A:A', 30)
worksheet3.set_column('B:B', 30)
worksheet3.set_column('C:C', 30)
worksheet3.set_column('D:D', 30)
worksheet3.set_column('E:E', 30)
worksheet3.set_column('F:F', 30)
worksheet3.set_column('G:G', 30)
worksheet3.set_column('H:H', 30)
worksheet3.set_column('I:I', 30)   
worksheet3.set_column('J:J', 30)

worksheet4.set_column('A:A', 30)
worksheet4.set_column('B:B', 30)
worksheet4.set_column('C:C', 30)
worksheet4.set_column('D:D', 30)
worksheet4.set_column('E:E', 30)
worksheet4.set_column('F:F', 30)
worksheet4.set_column('G:G', 30)
worksheet4.set_column('H:H', 30)
worksheet4.set_column('I:I', 30)   
worksheet4.set_column('J:J', 30)

worksheet5.set_column('A:A', 30)
worksheet5.set_column('B:B', 30)
worksheet5.set_column('C:C', 30)
worksheet5.set_column('D:D', 30)
worksheet5.set_column('E:E', 30)
worksheet5.set_column('F:F', 30)
worksheet5.set_column('G:G', 30)
worksheet5.set_column('H:H', 30)
worksheet5.set_column('I:I', 30)   
worksheet5.set_column('J:J', 30)

worksheet6.set_column('A:A', 30)
worksheet6.set_column('B:B', 30)
worksheet6.set_column('C:C', 30)
worksheet6.set_column('D:D', 30)
worksheet6.set_column('E:E', 30)
worksheet6.set_column('F:F', 30)
worksheet6.set_column('G:G', 30)
worksheet6.set_column('H:H', 30)
worksheet6.set_column('I:I', 30)   
worksheet6.set_column('J:J', 30)

worksheet7.set_column('A:A', 30)
worksheet7.set_column('B:B', 30)
worksheet7.set_column('C:C', 30)
worksheet7.set_column('D:D', 30)
worksheet7.set_column('E:E', 30)
worksheet7.set_column('F:F', 30)
worksheet7.set_column('G:G', 30)
worksheet7.set_column('H:H', 30)
worksheet7.set_column('I:I', 30)   
worksheet7.set_column('J:J', 30)

writer.save()
