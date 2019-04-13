import requests
import pandas as pd
# import numpy as np
import datetime
import json
import yaml
import time

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table as dt
import plotly.graph_objs as go

from kafka import KafkaProducer

from flask import Flask

with open("conf.yaml", "r") as ymlfile:
    cfg = yaml.load(ymlfile)
    jcdecaux_api_key = cfg["jcdecaux"]["api_key"]
    map_token = cfg["mapbox"]["map_token"]
    mapbox_public_token = cfg["mapbox"]["default_public_token"]

url_stations = 'https://api.jcdecaux.com/vls/v1/stations?apiKey=' + jcdecaux_api_key
url_contract = 'https://api.jcdecaux.com/vls/v1/stations?contract?apiKey=' + jcdecaux_api_key

def refresher():
    return dcc.Interval(
        id='interval-component',
        interval=10000, # 10.000 milliseconds = 10 seconds
        n_intervals=0
        )

def get_stations(url_stations):
       r = requests.get(url_stations)
       # print('status code : ', r.status_code)
       rawData = r.content.decode('utf-8')
       data_stations = pd.read_json(rawData)
       # print(data_stations.columns.values)
       data_stations.iloc[:,7] = data_stations.iloc[:,7].map(lambda x : datetime.datetime.fromtimestamp(x/1000.0))
       data_stations = data_stations.assign(lat= data_stations.position.map(lambda x : x['lat']), long= data_stations.position.map(lambda x : x['lng']))
       # data_stations = data_stations.reindex(columns=['address', 'available_bike_stands', 'available_bikes', 'banking',
       # 'bike_stands', 'bonus', 'contract_name', 'last_update', 'name',
       # 'number', 'position', 'lat', 'long', 'status'])
       data_stations.rename(columns={'contract_name':'city'}, inplace=True)
       data_stations.city = data_stations.city.map(lambda x : x.capitalize())
       data_stations = data_stations[['city','name','available_bike_stands','available_bikes','bike_stands','status','last_update', 'lat', 'long']]
       return data_stations

# df = pd.DataFrame(np.random.randint(0,100,size=(100, 4)), columns=list('ABCD'))

def generate_table(dataframe):
    return dt.DataTable(
    id='datatable',
    columns=[{"name": i, "id": i} for i in dataframe.columns],
    data=dataframe.to_dict("rows"),
    filtering=False,
    sorting=True,
    sorting_type="multi",
#     row_selectable="multi",
    row_deletable=False,
    selected_rows=[],
    pagination_mode="fe",
        pagination_settings={
               "displayed_pages": 1,
               "current_page": 0,
               "page_size": 10,
            },
            navigation="page",
)

def generate_bar_city(df):
    return dcc.Graph(
    figure=go.Figure(
    data=[
    go.Bar(
        x=df.index,
        y=df.available_bikes,
        name='# Available Bikes',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.index,
        y=df.available_bike_stands,
        name='# Available Bike Stands',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.index,
        y=df.available_bike_stands,
        name='# Distinct Access Point ',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.index,
        y=df.bike_stands,
        name='Total Bike Stands',
        marker=go.bar.Marker()
    ),
],
    layout=go.Layout(
    title='View by City',
    showlegend=True,
    barmode='group',
#     xaxis = dict(tickvals=df.index)
    )
))

def overall_figures():
       return #
# Available Bikes
# Available Bikes Stands
# Distinct Access Point
# Total Bike Stands


def generate_dropdown():
    return html.Div([
        dcc.Dropdown(
            id='dropdown',
            options=city_options,
            value=[],
            multi=True
        ),
        html.Div(style= {'margin':'10px'})
    ])

def generate_range_slider():
    return html.Div([
        dcc.RangeSlider(
        id='range-slider',
        min=0,
        max=70,
        value=[0,50],
        marks={
              '0': {'label': '0', 'style': {'color': '#f50'}},      # red
              '2': {'label': '2', 'style': {'color': 'orange'}},    # orange
              '5': {'label': '5', 'style': {'color': '#FFFF66'}},   # yellow
              '10': {'label': '10', 'style': {'color': 'blue'}},    # blue
              '20': {'label': '20'},
              '40': {'label': '40'},
              '70': {'label': '70', 'style': {'color': '#77b0b1'}}  # green
        },
        ),
        html.Div(style= {'margin':'30px'})
    ])

def generate_map(df_updated):
    colors = {'open':'green', 'closed':'red'}
    return html.Div([
    html.H2('Bike Stands Map'),
    html.Div(id='text-content', style= {'color': 'blue', 'fontSize': 15}),
    dcc.Graph(id='map', figure={
        'data': [{
            'lat': df_updated['lat'].loc[df_updated.status == 'OPEN'],
            'lon': df_updated['long'].loc[df_updated.status == 'OPEN'],
            'marker': {
                'color': colors['open'],
                'size': df_updated['bike_stands'].loc[df_updated.status == 'OPEN'],
                'opacity': 0.6,
            },
            'mode': 'markers',
            'name': 'OPEN',
            'text': df_updated['bike_stands'].loc[df_updated.status == 'OPEN'],
            'customdata': df_updated['name'].loc[df_updated.status == 'OPEN'],
            'type': 'scattermapbox'
        },
        {
            'lat': df_updated['lat'].loc[df_updated.status == 'CLOSED'],
            'lon': df_updated['long'].loc[df_updated.status == 'CLOSED'],
            'marker': {
                'color': colors['closed'],
                'size': df_updated['bike_stands'].loc[df_updated.status == 'CLOSED'],
                'opacity': 0.6
            },
            'name': 'CLOSED',
            'customdata': df_updated['name'].loc[df_updated.status == 'OPEN'],
            'type': 'scattermapbox'
        }],
        'layout': {
            'mapbox': {
                'accesstoken': mapbox_public_token
            },
            'hovermode': 'closest',
            'margin': {'l': 0, 'r': 0, 'b': 0, 't': 0}
        }
    })
])


markdown_text = '''
### About the project

##### *Dockerized Dash app on ECS with Kafka distributed treatment*

Données des systèmes de location de vélos en libre-service dans le monde développés par JCDecaux - récupérées via leur API.

L'objectif était la réalisation d'une architecture Big Data. \
Nous souhaitions réaliser un dashboard exploitant des données en temps réel dans une architecture distribuée. \
Nous nous sommes rapidement orientés vers une architecture dite en lambda, gérant les flux de données massives à la fois en bash et 
en streaming. Nous souhaitions faire tourner l'application sur un cluster EC2 mais nous ne maîtrisions pas les coûts et la facture nous en a dissuadé.
Nous avons donc décidé de développer l'outil en local et de simuler une architecture distribuée grâce aux containers Docker. \
Les flux de données sont gérés par Kafka qui redistribuent les données sur 2 noeuds.
Enfin, nous avons pu déployer l'application sur ECS (Elastic Container Service) d'AWS.

NB: Pour limiter les appels à l'API, nous avons décidé d'actualiser les données toutes les 10 secondes mais techniquement, ECS 
permet la montée en charge grâce au load balancing.

'''

n_intervals = 0
counter = 0
counter_list = []
df = get_stations(url_stations)
df_updated = df

city_options = []
city_ordered = sorted(df['city'].unique())
for city in city_ordered:
    city_options.append({'label':str(city), 'value':str(city)})

df_city = df[['city', 'available_bike_stands', 'available_bikes', 'bike_stands']]
df_city = df.groupby('city').agg({'available_bike_stands':'sum','available_bikes':'sum','bike_stands':'sum','status':'count'})

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)


app.layout = html.Div(children=[
        refresher(),
        html.P(id='live-update-text'),
        html.H1('JCDecaux Worldwide Bike Rantal Dashboard', style=dict(textAlign = 'center')),
        html.H4('Projet de Big Data Architecture', style=dict(textAlign = 'center')),
        dcc.Markdown(markdown_text),
        html.H2(id='counter_text', style={'fontWeight':'bold'}),
        html.Label('City Filter'),
        generate_dropdown(),
        html.Div(id='slider-output-container'),
        generate_range_slider(),
        generate_table(df),
        generate_bar_city(df_city),
        overall_figures(),
        dcc.Graph(id='live-update-graph',style={'width':1200}),
        generate_map(df_updated),

])


@app.callback(Output('counter_text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_data(n_intervals):
    global df_updated
    df_updated = get_stations(url_stations)
    # print('2 ', counter_list)
    return '# Worldwide active bikers: {}'.format(counter)

@app.callback(Output('datatable', 'data'),
              [Input('dropdown', 'value'),
              Input('range-slider', 'value')
              ])
def update_datatable(selected_city, available_bikes_range):
    if len(selected_city) == 0:
        df_updated = df[(df['available_bikes'] >= available_bikes_range[0]) & (df['available_bikes'] <= available_bikes_range[1]) ]
    else:
        df_updated = df[(df['city'].isin(selected_city)) & (df['available_bikes'] >= available_bikes_range[0]) & (df['available_bikes'] <= available_bikes_range[1]) ]
    return df_updated.to_dict('rows')

@app.callback(
    Output('slider-output-container', 'children'),
    [Input('range-slider', 'value')])
def update_output(value):
    return html.Label('# Available Bikes - You have selected between {} and {}'.format(value[0], value[1]))

@app.callback(Output('live-update-text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_layout(n_intervals): 
    # print('1 ', counter_list)
    return 'Refresh #{}'.format(n_intervals)

@app.callback(Output('live-update-graph','figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph(n_intervals):
    global counter
    counter = df_updated.available_bike_stands.sum()
    counter_list.append(counter) 
    fig = go.Figure(
        data = [go.Scatter(
        x = list(range(0, len(counter_list))),
        y = counter_list,
        mode='lines+markers'
        )])
    return fig

@app.callback(
    dash.dependencies.Output('text-content', 'children'),
    [dash.dependencies.Input('map', 'hoverData')])
def update_text(hoverData):
    s = df_updated[df_updated['name'] == hoverData['points'][0]['customdata']]
    # print(hoverData)
    # print(df_updated['lat'].loc[df_updated.status == 'OPEN'])
    return html.Div(
        'The contract {} has {} available bike stands, {} available bikes and is {}'.format(
            s.iloc[0]['name'],
            s.iloc[0]['available_bike_stands'],
            s.iloc[0]['available_bikes'],
            s.iloc[0]['status']
        )
    )


if __name__ == "__main__":
    app.run_server(debug=True)
