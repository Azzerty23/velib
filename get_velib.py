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
    return dcc.Dropdown(
              id='dropdown',
              options=city_options,
        value=[],
        multi=True
    )

def generate_range_slider():
    return dcc.RangeSlider(
        id='range-slider',
        min=0,
        max=69,
        value=[0,50],
#         marks={
#               0: {'label': 'Empty Bike Stand', 'style': {'color': '#f50'}},
#               20: {'label': '20'},
#               40: {'label': '40'},
#               69: {'label': '69', 'style': {'color': '#77b0b1'}}
#     },        value=[min, max],
        )

def generate_map(df_updated):
    return html.Div([
    html.H1('Bike Stands Map'),
    html.Div(id='text-content'),
    dcc.Graph(id='map', figure={
        'data': [{
            'lat': df_updated['lat'],
            'lon': df_updated['long'],
            'marker': {
                'color': df_updated['status'],
                'size': 8,
                'opacity': 0.6
            },
            'customdata': df_updated['name'],
            'type': 'scattermapbox'
        }],
        'layout': {
            'mapbox': {
                'accesstoken': mapbox_public_token # 'pk.eyJ1IjoiY2hyaWRkeXAiLCJhIjoiY2ozcGI1MTZ3MDBpcTJ3cXR4b3owdDQwaCJ9.8jpMunbKjdq1anXwU5gxIw'
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
        html.Label('# Available Bikes'),
        generate_range_slider(),
        html.Div(id='slider-output-container'),
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
    return 'You have selected "{}"'.format(value)

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
    return html.H3(
        'The contract {} has {} available bike stands, {} available bikes and is {}'.format(
            s.iloc[0]['name'],
            s.iloc[0]['available_bike_stands'],
            s.iloc[0]['available_bikes'],
            s.iloc[0]['status']
        )
    )


if __name__ == "__main__":
    app.run_server(debug=True)
