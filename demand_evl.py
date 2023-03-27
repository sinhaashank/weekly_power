#importing libraries
import pandas as pd
import numpy as np
import os
import plotly
import warnings

from datetime import date
from datetime import timedelta

import plotly.graph_objs as go
from plotly.subplots import make_subplots

# Ignoring Warnings
warnings.filterwarnings("ignore")

# loading database
from arctic import Arctic, CHUNK_STORE
conn = Arctic('localhost')
conn.initialize_library('entsoe', lib_type=CHUNK_STORE)
lib_entsoe = conn['entsoe']

# function to change timezone from UTC to local time
def changing_timezone(x):
    ts = x.index.tz_localize('utc').tz_convert('Europe/Brussels')
    y = x.set_index(ts)
    return y.tz_localize(None)

# loading historical data
df_hist = pd.read_pickle(os.path.join(os.getcwd(),'df_demand.p'))

# setting date

today = date.today()
ref_date = today + timedelta(days=-today.weekday(), weeks=-1)

start_date = (df_hist.index[-1] + timedelta(days = - 1)).date()
end_date = ref_date + timedelta(days=7)

df_demand = pd.DataFrame(columns=[])
perimeter = ['DE', 'FR','ES','BE', 'PL']
var = 'ActualTotalLoad_6.1.A'

# Read demand data
for i in perimeter: 
    read = lib_entsoe.read(var + '_' + i,chunk_range=pd.date_range(start_date, end_date))
    df_demand = pd.merge(df_demand, read, how = 'outer', left_index=True, right_index=True)
    
# convert 15 min data to hourly data
df_demand = df_demand.resample('H').mean()

# changing timezones 
df_demand = changing_timezone(df_demand)

df_demand=df_demand.loc[(df_demand.index.date<end_date)]

# joining dataframes
df=pd.concat([df_hist,df_demand])

# removing duplicates
df = df[~df.index.duplicated(keep='last')]

# saving histoical data
df.to_pickle('df_demand.p')

# changing to weekly granularity
df_w = df.resample('W').mean()

df_w['Week_Number'] = df_w.index.week

# plotting

#-----------------------------------------------------------------------------
fig = plotly.subplots.make_subplots(
        rows=3, cols=2, 
        subplot_titles = perimeter,
        shared_xaxes=False,
        vertical_spacing=0.1,
    )
#-----------------------------------------------------------------------------
var = ['min', 'max','mean']
for j in range(len(perimeter)): 
    if ((j+1) % 2) == 0:
        k = 2
    else:
        k = 1
    df_range = df_w.loc[df_w.index.year < today.year].groupby('Week_Number').agg({'ActualTotalLoad'+'_'+perimeter[j]:var})
    
    trace = go.Scatter(x=df_range.index, 
        y=df_range['ActualTotalLoad'+'_'+perimeter[j],var[0]],
        name = var[0],line_color='lightgrey',
                      showlegend=False,
                      #legendgroup='g1',
                      hovertemplate='%{x} , %{y:.0f}')
    fig.append_trace(trace, -(-(j+1)//2), k)
#-----------------------------------------------------------------------------
    trace = go.Scatter(x=df_range.index, 
        y=df_range['ActualTotalLoad'+'_'+perimeter[j],var[1]],
        name = 'min-max range',
        fill='tonexty',
        line_color='lightgrey',
        showlegend= False if j>0 else True,
        legendgroup='g2',
        hovertemplate='%{x} , %{y:.0f}')
    fig.add_trace(trace, -(-(j+1)//2), k)

    trace = go.Scatter(x=df_range.index, 
        y=df_range['ActualTotalLoad'+'_'+perimeter[j],var[2]],
        name = var[2] + '(2015-'+str(today.year-1)+')' ,
        line_color='royalblue',
        showlegend= False if j>0 else True,
        legendgroup='g3',
        hovertemplate='%{x} , %{y:.0f}')
    fig.append_trace(trace, -(-(j+1)//2), k)
#-------------------------------------------------------------------------    
    trace = go.Scatter(
        x = df_w['Week_Number'], 
        y = df_w.loc[df_w.index.year==today.year]['ActualTotalLoad'+'_'+perimeter[j]], 
        name = today.year,
        line = dict(color='red', width=3),
        showlegend= False if j>0 else True,
        legendgroup='g4',
        hovertemplate='%{x} , %{y:.0f}'
    )
    fig.add_trace(trace, -(-(j+1)//2), k)
#-----------------------------------------------------------------------------  
    trace = go.Scatter(
        x = df_w['Week_Number'], 
        y = df_w.loc[df_w.index.year==today.year-1]['ActualTotalLoad'+'_'+perimeter[j]], 
        name = today.year-1,
        line = dict(color= 'green',dash = 'dash'),
        showlegend= False if j>0 else True,
        legendgroup='g5',
        hovertemplate='%{x},%{y:.0f}'
    )
    fig.add_trace(trace, -(-(j+1)//2), k)
    
# Add figure title
fig.update_layout(
    title_text="EU Power Demand - Weekly Evolution")

# Set axes titles
fig.update_yaxes(title_text="Load (MW)", title_font = dict(size = 12))
fig.update_xaxes(dtick = 5)

outfile = 'EU_demand'+'.html'

fig.write_html(os.path.join(os.getcwd() + '/plots', outfile))