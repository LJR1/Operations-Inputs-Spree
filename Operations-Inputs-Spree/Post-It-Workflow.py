# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 11:54:27 2014

"""

import pandas as pd
from pandas import DataFrame
import gspread
import numpy as np
from datetime import datetime, timedelta
from pandas import ExcelWriter
import re
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab

params = {'legend.fontsize': 8,
          'legend.linewidth': 1,
          'font.weight': 'bold' 
          }
          
plt.rcParams.update(params)

pswd = raw_input("Please enter your google password:")
email_address= raw_input("Please enter your email address:")


c = gspread.Client(auth=(email_address,pswd))
c.login()
c.open('Stock Processing')
sht=c.open('Stock Processing')
worksheet = sht.worksheet('Post It_Workflow')
info=worksheet.get_all_values()
headers=info.pop(0)
df1=DataFrame(data=info,columns=headers)

d=gspread.Client(auth=(email_address,pswd))
d.login()
d.open('Epping Receiving Report')
sht=d.open('Epping Receiving Report')
worksheet = sht.worksheet('Booked')
info=worksheet.get_all_values()
headers=info.pop(0)
df2=DataFrame(data=info,columns=headers)

#Creating the last rolling 14 day PO received count dataframe
DailyReceived=df2[['Date received','POs']] 
DailyReceived['Date received']=pd.to_datetime(DailyReceived['Date received'],coerce=True)
x=DailyReceived.set_index(pd.DatetimeIndex(DailyReceived['Date received']))
y=x['POs']
grouped=y.groupby(level=0)
x=grouped.count()
Rolling_7_Day_Receives=x.ix[-15:]

#Creating the number of PO's processed per day 
DailyQC=df1[['POs','Sort and Count Start Date:','QC Start Date:','QC End Date:']] 
DailyQC['Sort and Count Start Date:']=pd.to_datetime(DailyQC['Sort and Count Start Date:'],coerce=True)
DailyQC['QC Start Date:']=pd.to_datetime(DailyQC['QC Start Date:'],coerce=True)
DailyQC['QC End Date:']=pd.to_datetime(DailyQC['QC End Date:'],coerce=True)
DailyQC= DailyQC[pd.notnull(DailyQC['QC Start Date:'])]
DailyQC['Days to process a PO']=DailyQC['QC End Date:']-DailyQC['QC Start Date:']

a=DailyQC.set_index(pd.DatetimeIndex(DailyQC['QC Start Date:']))
a['Days to process a PO']=a['Days to process a PO'].apply(lambda x: x / np.timedelta64(1,'D'))
a=a.fillna(-1)
a['Days to process a PO'].astype(int)
b=a['Days to process a PO']

groupb=b.groupby(level=0)
groupedQC=groupb.count()
Rolling_7_Day_QC=groupedQC.ix[-7:]

z=pd.concat([Rolling_7_Day_QC,Rolling_7_Day_Receives], join='outer',axis=1)
z = z.rename(columns = {'Days to process a PO':'QC Processed','POs':'Supplier Receives'})

#Define the product type QC'd per day
ProductProcessed=df1[['POs','QC Start Date:','Supplier Name:','Product Type']]

ProductProcessed['QC Start Date:']=pd.to_datetime(ProductProcessed['QC Start Date:'],coerce=True)
ProductProcessed= ProductProcessed[pd.notnull(ProductProcessed['QC Start Date:'])]
#ProductProcessed=ProductProcessed.set_index(pd.DatetimeIndex(ProductProcessed['QC Start Date:']))

ProductProcessedGraph=ProductProcessed[['QC Start Date:','Product Type']]

ProductProcessedGraph=ProductProcessedGraph[ProductProcessedGraph['QC Start Date:']>'2014-11-20']
ProductProcessedGraph['Quantity']=1

ProductList={
'LADIES CLOTHING': 'Ladies Clothes',
'MENS CLOTHING': 'Mens Clothes',
'SHOES': 'Shoes',
'ACCESSORIES (e.g. Bags, Belts)': 'Accessories',
'KIDS CLOTHING AND SHOES': 'Kids Clothes',
'BABIES CLOTHING AND SHOES': 'Kids Clothes',
'BEAUTY PRODUCTS': 'Beauty',
'DINING (e.g. Plates, Bowls)':'Home/Decor',
'BEDROOM AND BATHROOM (e.g. Blankets, Towels)': 'Home/Decor',
'HOME (e.g. Lighting, Cushions)': 'Home/Decor',
'JEWELLERY (e.g Necklace)': 'Accessories',
'DECORATIVE ACCESSORIES (e.g. Picture frame)': 'Accessories',
'PET ACCESSORIES (e.g Dog Beds)': 'Accessories',
'PAPER AND PARTY (e.g. Paper, Pens)': 'Accessories',
'OTHER': 'Other',
}

ProductProcessedGraphO=ProductProcessedGraph.ix[-7:]

ProductProcessedGraphO['Product Type']=ProductProcessedGraphO['Product Type'].map(ProductList)
ProductProcessedGraphO['QC Start Date:']=ProductProcessedGraphO['QC Start Date:'].apply(lambda x: x.date())

test=pd.pivot_table(ProductProcessedGraphO,index=['QC Start Date:'],columns=['Product Type'],values='Quantity',aggfunc=np.sum)

#Set Cumulative Sum Index of PO's for Receive/QC + Backlog as of the 29 October 2014
BacklogAmount=1
ReceiveIndex=Rolling_7_Day_Receives.ix[-15:].copy()
ReceiveIndex[:1]=BacklogAmount
ReceiveIndex=ReceiveIndex.cumsum()

QCIndex=Rolling_7_Day_QC.ix[-15:].copy()
QCIndex=QCIndex.cumsum()

M=pd.concat([ReceiveIndex,QCIndex], join='outer',axis=1)
M = M.rename(columns = {'POs':'PO Backlog','Days to process a PO':'POs Completed',})
    
def plot_mpl_fig():

    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(11,11))
    z.dropna().plot(ax=axes[0,0],marker='o',title='PO arrivals vs. QC Processed')
    M.dropna().plot(ax=axes[0,1],marker='o', title='Current PO Backlog')
    test.plot(ax=axes[1,0], kind='bar', stacked=True, title='Product type per PO')
    axes[0,0].legend(loc=3)
    axes[0,1].legend(loc=3)
    axes[1,0].legend(loc=3)
    fig.tight_layout()

    return(fig,axes)
    