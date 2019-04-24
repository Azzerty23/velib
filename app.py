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
import dash_auth

from flask import Flask

from kafka import KafkaProducer
from velib_monitor_stations import count_diff_stations


######## Ressources ########

with open("conf.yaml", "r") as ymlfile:
    cfg = yaml.load(ymlfile)
    dash_username = cfg['dash']['username']
    dash_password = cfg['dash']['password']
    jcdecaux_api_key = cfg["jcdecaux"]["api_key"]
    map_token = cfg["mapbox"]["map_token"]
    mapbox_public_token = cfg["mapbox"]["default_public_token"]

USERNAME_PASSWORD_PAIRS = [[dash_username, dash_password]]

url_stations = 'https://api.jcdecaux.com/vls/v1/stations?apiKey=' + jcdecaux_api_key
url_contract = 'https://api.jcdecaux.com/vls/v1/stations?contract?apiKey=' + jcdecaux_api_key
url_kafka_consumer = 'localhost:9092'
url_kafka_consumer2 = 'localhost:9093'


######## Functions ########

def refresher():
    return dcc.Interval(
        id='interval-component',
        interval=2000, # 2.000 milliseconds = 2 seconds
        n_intervals=0
        )

def get_stations(url_stations):
       r = requests.get(url_stations)
       # print('status code : ', r.status_code)
       rawData = r.content.decode('utf-8')

       if producer != 0:
           stations = json.loads(rawData) # json.loads(r.read().decode())
           for station in stations:
               producer.send("velib-stations", json.dumps(station).encode())
            #    print("{} Produced {} station records".format(time.time(), len(stations)))
            #    time.sleep(10)
           
        #    r1 = requests.get(url_kafka_consumer)
        #    r1 = r1.content.decode('utf-8')
        #    data_consumer1 = pd.read_json(r1)

        #    r2 = requests.get(url_kafka_consumer2)
        #    r2 = r2.content.decode('utf-8')
        #    data_consumer2 = pd.read_json(r2)
        #    data_consumer = data_consumer1.append(data_consumer2)

        #    print(data_consumer1)
        #    return data_consumer1

       data_stations = pd.read_json(rawData)
       data_stations.iloc[:,7] = data_stations.iloc[:,7].map(lambda x : datetime.datetime.fromtimestamp(x/1000.0))
       data_stations = data_stations.assign(lat= data_stations.position.map(lambda x : x['lat']), long= data_stations.position.map(lambda x : x['lng']))
       # data_stations = data_stations.reindex(columns=['address', 'available_bike_stands', 'available_bikes', 'banking',
       # 'bike_stands', 'bonus', 'contract_name', 'last_update', 'name',
       # 'number', 'position', 'lat', 'long', 'status'])
       data_stations.rename(columns={'contract_name':'city'}, inplace=True)
       data_stations.city = data_stations.city.map(lambda x : x.capitalize())
       data_stations = data_stations[['city','name','available_bike_stands','available_bikes','bike_stands','status','last_update', 'lat', 'long']]
       return data_stations

def generate_table(df):
    df = df.iloc[:,:7]
    return dt.DataTable(
    id='datatable',
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict("rows"),
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
               "page_size": 20,
            },
            navigation="page",
    style_as_list_view=False,
    style_data_conditional=[
        {
            'if': {
                'column_id': str(i),
                'filter': 'available_bikes = num(0)',
            },
        'color': 'red',
        } for i in df.columns
    ],
    style_table={'height': 400, 'overflowY': 'scroll'},
    # n_fixed_rows= 1,
)

def generate_bar_city(df):
    return dcc.Graph(
    id='bar_city',
    figure=go.Figure(
    data=[
    go.Bar(
        x=df.city,
        y=df.status,
        name='# Distinct Access Point ',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.city,
        y=df.available_bikes,
        name='# Available Bikes',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.city,
        y=df.available_bike_stands,
        name='# Available Bike Stands',
        marker=go.bar.Marker()
    ),
    go.Bar(
        x=df.city,
        y=df.bike_stands,
        name='Total Bike Stands',
        marker=go.bar.Marker()
    ),
],
    layout=go.Layout(
    title='View by City',
    showlegend=True,
    barmode='group',
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

def generate_radio_items_order_city():
    return dcc.RadioItems(
            id='radioItems',
            options=[
                {'label': 'A-Z Order', 'value': 'AZ'},
                {'label': 'Filter Order', 'value': 'filter'},
            ],
            value='AZ',
            labelStyle={'display': 'inline-block', 'marginRight': '10px'},
            style= {'textAlign': 'center'}) # 'wordSpacing': '10px'
    # ])

def generate_alert():
    return dcc.ConfirmDialog(
        id='confirm',
        message='This option is not deployed yet'
    )

def generate_map(df_updated):
    colors = {'open':'#2E8B57', 'closed':'#B22222'} # green / red
    return html.Div([
    html.Div('Contract infos', id='text-content', style= {'color': '#1E90FF', 'fontSize': 15}),
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
            'text': df_updated['name'].loc[df_updated.status == 'OPEN'],
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
            'text': df_updated['name'].loc[df_updated.status == 'CLOSED'],
            'customdata': df_updated['name'].loc[df_updated.status == 'CLOSED'],
            'type': 'scattermapbox'
        }],
        'layout': {
            'title': 'Bike Stands Map',
            'mapbox': {
                'accesstoken': mapbox_public_token,
                'bearing': 0,
                'center': {'lat': 45, 'lon': 3},
                'pitch': 50, 'zoom': 3, 
                # 'minzoom':1,
                # 'maxzoom':1,
                # "style": 'mapbox://styles/mapbox/light-v9' # v9
            },
            'hovermode': 'closest',
            'margin': {'l': 0, 'r': 0, 'b': 0, 't': 30},
            'height': 700,
            # 'tickformatstops': {'dtickrange': [0,5]},
            # 'dtickrange': [0,1]
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


######## Kafka Producer ########

try:
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
except:
    producer = 0
    print('kafka not launched')



######## Initialisation des variables ########

n_intervals = 0
counter = 0
counter_list = []
df = get_stations(url_stations)
df_updated = df

city_options = []
city_ordered = sorted(df['city'].unique())
for city in city_ordered:
    city_options.append({'label':str(city), 'value':str(city)})

# df_city = df[['city', 'available_bike_stands', 'available_bikes', 'bike_stands']]
df_city = df.groupby('city', as_index=False).agg({'available_bike_stands':'sum','available_bikes':'sum','bike_stands':'sum','status':'count'})


######## App core ########

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

server = Flask(__name__)
app = dash.Dash(__name__, server=server, external_stylesheets=external_stylesheets)
auth = dash_auth.BasicAuth(app, USERNAME_PASSWORD_PAIRS)

app.layout = html.Div(children=[
        refresher(),
        generate_alert(),
        html.P(id='live-update-text'),
        html.H1('JCDecaux Worldwide Bike Rantal Dashboard', style=dict(textAlign = 'center')),
        html.H4('Projet de Big Data Architecture', style=dict(textAlign = 'center')),
        dcc.Markdown(markdown_text),
        html.H2(id='counter_text', style={'fontWeight':'bold'}),
        # count_diff_stations(producer),
        # html.Div(id='live-count-diff'),
        html.Label('City Filter'),
        generate_dropdown(),
        html.Div(id='slider-output-container'),
        generate_range_slider(),
        generate_table(df),
        generate_bar_city(df_city),
        generate_radio_items_order_city(),
        overall_figures(),
        dcc.Graph(id='live-update-graph'),
        generate_map(df_updated),

])


######## Callbacks ########

@app.callback(Output('counter_text', 'children'),
              [Input('interval-component', 'n_intervals')])
def update_data(n_intervals):
    global df_updated
    df_updated = get_stations(url_stations)
    total_bike_stands = df.bike_stands.sum()
    # print('2 ', counter_list)
    return '# Worldwide active bikers: {} / {}'.format(counter, total_bike_stands)


# @app.callback(Output('live-count-diff', 'children'),
#               [Input('interval-component', 'n_intervals')])
# def update_count_diff(n_intervals):
#     count_diff = count_diff_stations(producer)
#     return 'count_diff: {}'.format(count_diff)

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

@app.callback(Output('bar_city', 'figure'),
              [Input('dropdown', 'value'),
              Input('range-slider', 'value')
              ])
def update_bar_chart(selected_city, available_bikes_range):
    if len(selected_city) != 0:
        df_updated = df[(df['city'].isin(selected_city)) & (df['available_bikes'] >= available_bikes_range[0]) & (df['available_bikes'] <= available_bikes_range[1]) ]
        df_updated = df_updated.groupby('city', as_index=False).agg({'available_bike_stands':'sum','available_bikes':'sum','bike_stands':'sum','status':'count'})
    else:
        df_updated = df_city

    return {
        'data': [
            go.Bar(
                x=df_updated.city,
                y=df_updated.status,
                name='# Distinct Access Point ',
                marker=go.bar.Marker()
            ),
            go.Bar(
                x=df_updated.city,
                y=df_updated.available_bikes,
                name= '# Available Bikes',
                marker= go.bar.Marker()
            ),
            go.Bar(
                x=df_updated.city,
                y=df_updated.available_bike_stands,
                name='# Available Bike Stands',
                marker=go.bar.Marker()
            ),
            go.Bar(
                x=df_updated.city,
                y=df_updated.bike_stands,
                name='Total Bike Stands',
                marker=go.bar.Marker()
            ),
        ],
        'layout': go.Layout(
        title='View by City',
        showlegend=True,
        barmode='group',
        )
    }

@app.callback(Output('confirm', 'displayed'),
              [Input('radioItems', 'value')])
def display_confirm(value):
    if value == 'filter':
        return True
    return False

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
        mode='lines+markers+text',
        text= counter_list,
        textposition='top center',
        hoverinfo= 'x'
        )],
        layout=go.Layout(
        title='Worldwide Active Bikers Evolution Real-Time',
        # margin=go.layout.Margin(l=0, pad=50)
        )
    )
    return fig

@app.callback(
    dash.dependencies.Output('text-content', 'children'),
    [dash.dependencies.Input('map', 'hoverData')])
def update_text(hoverData):
    try:
        s = df_updated[df_updated['name'] == hoverData['points'][0]['customdata']]
        return html.Div(
            'The contract {} has {} available bike stands, {} available bikes and is {}'.format(
                s.iloc[0]['name'],
                s.iloc[0]['available_bike_stands'],
                s.iloc[0]['available_bikes'],
                s.iloc[0]['status']
            )
        )
    except:
        pass


if __name__ == "__main__":
    app.run_server(debug=True)
