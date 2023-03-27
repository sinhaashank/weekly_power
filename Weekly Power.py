
# importing libraries
import numpy as np
import pandas as pd

import os
import sys

from datetime import date
from datetime import timedelta

import plotly
import plotly.offline as pyo
import plotly.graph_objs as go
from plotly.subplots import make_subplots

import warnings
warnings.filterwarnings("ignore") 

# connecting to database

from arctic import Arctic, CHUNK_STORE

conn = Arctic('localhost')
lib_entsoe = conn['entsoe']

# function to change timezone from UTC to local time

def changing_timezone(x):
    ts = x.index.tz_localize('utc').tz_convert('Europe/Brussels')
    y = x.set_index(ts)
    return y.tz_localize(None)

# parameters
perimeter = ['DE','FR', 'BE', 'ES', 'PL']
      
today = date.today()
ref_date = today + timedelta(days=-today.weekday(), weeks=-1)
start_date = ref_date + timedelta(days=-15)
end_date = ref_date + timedelta(days=7)

def load_data(perimeter,start_date,end_date):    
    
    var1 = 'DayAheadPrices_12.1.D'
    var2 = 'ActualTotalLoad_6.1.A'
    var3 = 'AggregatedGenerationPerType_16.1.B_C'
    var4 = 'PhysicalFlows_12.1.G'
    
    df_1 = lib_entsoe.read(var1 + '_' + perimeter, chunk_range=pd.date_range(start_date, end_date))
    df_2 = lib_entsoe.read(var2 + '_' + perimeter, chunk_range=pd.date_range(start_date, end_date))
    df_3 = lib_entsoe.read(var3 + '_' + perimeter, chunk_range=pd.date_range(start_date, end_date))
    
    # Read cross border flows
    
    df_exports = pd.DataFrame(columns=[])
    df_imports = pd.DataFrame(columns=[])
        
    if perimeter == 'DE':
        interco = ['AT','BE','CZ','DK','FR','LU','NL','PL', 'SE','CH']
    elif perimeter == 'FR':
        interco = ['BE','DE','IT','ES','CH', 'GB']
    elif perimeter == 'BE':
        interco = ['FR','DE','LU','NL', 'GB']
    elif perimeter == 'ES':
        interco = ['FR','PT']
    elif perimeter == 'IT':
        interco = ['AT','GR','FR','MT','ME','SI','CH']
    elif perimeter == 'NL':
        interco = ['BE','DK','DE','NO','GB']
    elif perimeter == 'PL':
        interco = ['CZ','DE','LT','SK','SE']
    elif perimeter == 'GB':
        interco = ['BE','FR','IE','NL']
    
    for j in interco:
        # exports
        prefix = var4 + '_' + perimeter + '_' + j
        try:
            out_flows = lib_entsoe.read(prefix, chunk_range=pd.date_range(start_date, end_date))
            df_exports = pd.merge(df_exports,out_flows ,how='outer',right_index=True, left_index=True)    
        except Exception:
            pass    
        # imports
        prefix = var4 + '_' + j + '_' + perimeter   
        try:
            in_flows = lib_entsoe.read(prefix, chunk_range=pd.date_range(start_date, end_date))
            df_imports = pd.merge(df_imports,in_flows ,how='outer',right_index=True, left_index=True) 
        except Exception:
            pass
        
    flows = df_imports.subtract(df_exports.values).sum(axis =1, skipna= True)
    
    df_4 = pd.DataFrame(flows, columns = ['Net Imports'])
    
    # merging data to a single dataframe
    
    df_merge = pd.DataFrame(columns=[])
    
    for df in [df_1,df_2,df_3,df_4]:
        df_merge = pd.merge(df_merge, df,how='outer',right_index=True, left_index=True)
    
    # changing timezones 
    
    df_merge = changing_timezone(df_merge)
    
    # convert 15 min data to hourly data
    df_merge = df_merge.resample('H').mean() 
    
    #df_merge.index = pd.to_datetime(df_merge.index)
    
    df_merge=df_merge.loc[(df_merge.index.date>start_date)&(df_merge.index.date<end_date)]
        
    for i in ['ActualConsumption','Biomass','Waste','Other renewable', 'Oil', 'Geothermal', 'Other','Marine',
             'Coal-derived gas','Peat']:
    
        df_merge = df_merge[df_merge.columns.drop(list(df_merge.filter(regex=i)))]
        
    df_merge.columns = df_merge.columns.str.replace(r'DayAheadPrices_'+perimeter, 'Spot price')
        
    df_merge.columns = df_merge.columns.str.replace(r'ActualGenerationOutput ' + perimeter + ' ', '')
    df_merge.columns = df_merge.columns.str.replace(r'ActualTotalLoad_'+ perimeter, 'Demand')
    df_merge.columns = df_merge.columns.str.replace(r'Fossil Gas', 'Gas')
    df_merge.columns = df_merge.columns.str.replace(r'Fossil Hard coal', 'Coal')

    df_merge.columns = df_merge.columns.str.replace(r'Hydro Water Reservoir', 'Hydro Res')
    df_merge.columns = df_merge.columns.str.replace(r'Hydro Run-of-river and poundage', 'Hydro R-o-R')
    df_merge.columns = df_merge.columns.str.replace(r'Hydro Pumped Storage', 'Hydro Pump')
    
    try:
        df_merge.columns = df_merge.columns.str.replace(r'Fossil Brown coal/Lignite', 'Lignite')
        
    except KeyError:
        pass
    
    try:
        df_merge['Residual load'] = df_merge['Demand'] -(df_merge['Solar'] + df_merge['Wind Onshore'] + df_merge['Wind Offshore'])
        df_merge['RES_pen'] = ((df_merge['Solar'] + df_merge['Wind Onshore'] + df_merge['Wind Offshore'])/df_merge['Demand'])*100
    except KeyError:
        df_merge['Residual load'] = df_merge['Demand'] -(df_merge['Solar'] + df_merge['Wind Onshore'])
        df_merge['RES_pen'] = ((df_merge['Solar'] + df_merge['Wind Onshore'])/df_merge['Demand'])*100

    return df_merge


def prepare_weekly_data(perimeter, start_date, end_date):
    dF = pd.DataFrame()
    dg = load_data(perimeter, start_date,end_date)

    dg.to_csv(os.path.join(os.getcwd()  + '/data/daily',perimeter+'_daily.csv'))
    
    dg = dg.resample('W').mean()
    dg.index = dg.index.week
    dg = dg.rename('W/{}'.format)
    
    dg.to_csv(os.path.join(os.getcwd()  + '/data/weekly',perimeter+'_weekly.csv'))
    
    dF = dF.append(dg)
    
    dF.loc[:,~dF.columns.isin(['Spot price', 'RES_pen'])]/=1000

    dF=dF.transpose()
    dF=dF.reset_index()
    dF['\u0394 (W-o-W)'] = dF.iloc[:,-1] - dF.iloc[:,-2]
    dF = dF.rename({'index': ''},axis =1)

    return dF

def prepare_hourly_data(perimeter, ref_date, end_date):
    start_date = ref_date + timedelta(days=-1)
    df = load_data(perimeter, start_date,end_date)
    
    df.loc[:,~df.columns.isin(['Spot price', 'RES_pen'])]/=1000

    return df

fig = plotly.subplots.make_subplots(
        rows=3, cols=2, 
        subplot_titles = perimeter,
        shared_xaxes=False,
        vertical_spacing=0.1,
        specs=[[{"type": "table"}, {'type' : 'table'}],
           [{"type": "table"}, {'type' : 'table'}],
           [{"type": "table"}, {'type' : 'table'}]]
)
#-----------------------------------------------------------------------------

headerColor = 'light blue'
rowEvenColor = 'lightgrey'
rowOddColor = 'white'

for j in range(len(perimeter)): 
    if ((j+1) % 2) == 0:
        k = 2
    else:
        k = 1
    df = prepare_weekly_data(perimeter[j], start_date, end_date)
    data = go.Table(header=dict(values=list(df.columns),
                fill_color=headerColor,
                align=['left','center'],
                ),
                cells=dict(values=df.transpose(),
                fill_color = [[rowOddColor,rowEvenColor]*9],
                align=['left','center'],
                format = ['','.1f']
              )) 
    fig.add_trace(data, -(-(j+1)//2), k)

# Add figure title

fig.update_layout(
    title_text="Weekly Power Report (" + ref_date.strftime('%d-%b/%Y') + " - " + (end_date-timedelta(days=1)).strftime('%d-%b/%Y') + ")",
    height=1100
)

filename = os.path.join(os.getcwd()  + '/plots', 'Weekly Power Report - W'+ (end_date-timedelta(days=1)).strftime('%V-%Y') + '.html')

f = open(filename,"w")  # append mode 
f.write(fig.to_html(full_html=False))

gen_dict = {
    'Nuclear' : 'indianred',
    'Coal': 'brown',
    'Lignite' : 'saddlebrown',
    'Gas' : 'silver',
    'Hydro R-o-R' : 'blue',
    'Hydro Pumped' : 'orange',
    'Hydro Res' : 'plum',
    'Solar' : 'gold',
    'Wind Offshore' : 'green',
    'Wind Onshore': 'steelblue'
    }

countries_dict = {
  "DE": "indianred",
  "FR": "royalblue",
  "BE": "rosybrown",
  "ES": "tomato",
  "IT": "green",
  "NL": "orange",
  "PL": "silver", 
}


fig = plotly.subplots.make_subplots(
    rows=7, cols=1, row_heights=[0.22, 0.13, 0.13, 0.13, 0.13, 0.13, 0.13],
    subplot_titles = (['Spot Price'] + perimeter),
    shared_xaxes=False,
    vertical_spacing=0.05
)

for j in range(len(perimeter)):
    
    df = prepare_hourly_data(perimeter[j], ref_date, end_date)
    # Spot prices
    var = 'Spot price'
    
    try:
        trace = go.Scatter(x = df.index, 
                       y = df[var], 
                       name = perimeter[j],
                       line_color = countries_dict[perimeter[j]])
        fig.append_trace(trace, 1, 1)
    except KeyError:
        pass

for j in range(len(perimeter)):
    
    df = prepare_hourly_data(perimeter[j], ref_date, end_date)
    # Generation
  
    for i in gen_dict.keys():
        try:
            trace = go.Bar(x = df.index, 
                       y = df[i], 
                       name = i,
                       marker_color = gen_dict[i],
                       hovertemplate='%{x},%{y:.1f}',
                       legendgroup = i,
                       showlegend= False if j>0 else True
                          )
            fig.append_trace(trace, j+2, 1)
        except KeyError:
            pass
  
    # CrossBorder Trade
  
    var = 'Net Imports'
    trace = go.Bar(x = df.index, 
                   y = df[var], 
                   name = 'Imports/Exports',
                   marker_color = 'orchid',
                   hovertemplate='%{x},%{y:.1f}',
                   legendgroup = 'g1',
                   showlegend= False if j>0 else True,
)

    fig.add_trace(trace, j+2, 1)
  
    # Demand
  
    var = 'Demand'
    trace = go.Scatter(x = df.index, 
                       y = df[var], 
                       name = 'Demand',
                       visible = 'legendonly',
                       line = dict(color='black', width=3),
                       hovertemplate='%{x},%{y:.1f}',
                       legendgroup = 'g2',
                       showlegend= False if j>0 else True)
    
    fig.add_trace(trace, j+2, 1)    
    

fig.update_layout(
    title_text = "Hourly Stack",
    barmode='relative',
    bargap=0,
    height=1600,
    
    #xaxis=dict(autorange=True),
  
    yaxis1= dict(
       anchor = "x",
       autorange = True,
       title_text = "€/MWh"),

    yaxis2= dict(
       anchor = "x",
       autorange = True,
       #range = [-10,70],
       title_text = "GWh/h"),
    
     yaxis3= dict(
       anchor = "x",
       autorange = True,
       title_text = "GWh/h"),

    yaxis4= dict(
       anchor = "x",
       autorange = True,
       title_text = "GWh/h"),
    
    yaxis5= dict(
       anchor = "x",
       autorange = True,
       title_text = "GWh/h"),

    yaxis6= dict(
       anchor = "x",
       autorange = True,
       title_text = "GWh/h"),
    
     yaxis7= dict(
       anchor = "x",
       autorange = True,
       title_text = "GWh/h"))
                       
note = 'Source: ENTSOE<br>@SinhaAshank'
fig.add_annotation(
    showarrow=False,
    text=note,
    font=dict(size = 8), 
    xref='x domain',
    x=0.5,
    yref='y domain',
    y=-0.5
    )

f.write(fig.to_html(full_html=False))
f.close()


import demand_evl
f = open(filename,"a")  # append mode 
f.write(demand_evl.fig.to_html(full_html=False))
f.close()

# automate email

# import win32com.client as win32
# outlook = win32.Dispatch('outlook.application')
# mail = outlook.CreateItem(0)
# mail.To = 'AMEfront@arcelormittal.com;AMEmiddle@arcelormittal.com;Can.TURKTAS@arcelormittal.com'
# mail.cc = 'sebastien.percerou@arcelormittal.com;benjamin.lemoine@arcelormittal.com;rolf.berger@arcelormittal.com'
# mail.Subject = 'Weekly Power Report - W'+ (end_date-timedelta(days=1)).strftime('%V-%Y')
# mail.Body = """Hello,

# Find enclosed the latest weekly power report.

# Best,
# Ashank"""

#mail.HTMLBody = '<h2>HTML Message body</h2>' #this field is optional

# To attach a file to the email (optional):

attachment = os.path.join(os.getcwd()  + '/plots', 'Weekly Power Report - W'+ (end_date-timedelta(days=1)).strftime('%V-%Y') + '.html')
#attachment  = 'C:/Users/A0743104/OneDrive - ArcelorMittal/Desktop/Python Tools/Weekly Power/plots/' + 'Weekly Power Report - W'+ (end_date-timedelta(days=1)).strftime('%V-%Y') + '.html'

# mail.Attachments.Add(attachment)

# x = input('Check html file in the plots folder, press Y to proceed or N to abort - ')

# if x == 'Y' or x == 'y':
#     mail.Send()
# else:
#     pass
