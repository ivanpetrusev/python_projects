##!/usr/bin/python
# -*- coding: utf-8 -*-
 
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from datetime import datetime as dt
 
import plotly.graph_objs as go
 
import pandas as pd
 
from sqlalchemy import create_engine
 
engine = create_engine('sqlite:////db/zen.db', echo = False)
 
db_config = {'user': 'my_user',
            'pwd': 'my_user_password',
            'host': 'localhost',
            'port': 5432,
            'db': 'zen'}
engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(db_config['user'],
                                                           db_config['pwd'],
                                                           db_config['host'],
                                                           db_config['port'],
                                                           db_config['db']))
 
#получаем сырые данные
query1 = '''
            SELECT *
            FROM dash_visits
        '''
dash_visits = pd.io.sql.read_sql(query1, con = engine)
dash_visits['dt'] = pd.to_datetime(dash_visits['dt'])
 
query2 = '''
            SELECT *
            FROM dash_engagement
        '''
dash_engagement = pd.io.sql.read_sql(query2, con = engine)
dash_engagement['dt'] = pd.to_datetime(dash_engagement['dt'])
 
 
note = '''
          Этот дашборд показывает взаимодействие пользователей с карточками Яндекс.Дзен.
         
          Используйте выбор интервала даты события, тем карточек и возрастной категории пользователей для управления дашбордом.
       '''
#задаём лейаут
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
app.layout = html.Div(children=[  
 
    #заголовок
    html.H1(children = 'Мониторинг взаимодействия в Яндекс.Дзен'),    
 
    html.Br(),  
    html.Label(note),
 
    html.Br(),        
 
    html.Div([
        html.Div([
            html.Div([
            #выбор временного периода
                html.Label('Фильтр даты	'),
                    dcc.DatePickerRange(
                        start_date = dash_visits['dt'].dt.date.min(),
                        end_date = dt(2019,9,25).strftime('%Y-%m-%d'),
                        display_format = 'YYYY-MM-DD',
                        id = 'dt_selector',
                        ),
                    ], className = 'row container-display'),           
       
            html.Div([
            #выбор возраста
                html.Label('Фильтр возрастных категорий'),
                    dcc.Dropdown(
                        options = [{'label': x, 'value': x} for x in dash_visits['age_segment'].unique()],
                        value = dash_visits['age_segment'].unique(),
                        multi = True,
                        id = 'age-dropdown',
                        ),
                    ], className = 'row container-display'),
            ], className = 'six columns'
            ),
       
            html.Div([
            #выбор тем карточек    
                html.Label('Фильтр тем карточек'),
                    dcc.Dropdown(
                        options = [{'label': x, 'value': x} for x in dash_visits['item_topic'].unique()],
                        value = dash_visits['item_topic'].unique(),
                        multi = True,
                        id = 'item-topic-dropdown',
                        style = {'height':'50%'}
                        ),
                ], className = 'six columns'),
            ], className = 'row'), 
   
    html.Div([
        html.Div([        
            html.Label('История событий по темам карточек'),
            dcc.Graph(
                id = 'history-absolute-visits',
                style = {'height': '50vw'},
                ),  
            ], className = 'six columns'),  
 
        html.Div([        
            html.Label('События по темам источников'),
            dcc.Graph(
                id = 'pie-visits',
                style = {'height': '25vw'},
            ),  
            html.Label('Средняя глубина взаимодействия'),
            dcc.Graph(
                id = 'engagement-graph',
                style = {'height': '25vw'},
            ),  
            ], className = 'six columns')            
        ], className = 'row')
])
 
#описываем логику дашборда
@app.callback(
    [Output('history-absolute-visits', 'figure'),
    Output('pie-visits', 'figure'),
    Output('engagement-graph', 'figure')],
    
    [Input('item-topic-dropdown', 'value'),
    Input('age-dropdown', 'value'),
    Input('dt_selector', 'start_date'),
    Input('dt_selector', 'end_date'),
    ])
 
def update_figures(selected_item_topics, selected_ages, start_date, end_date):
 
    start_date = dt.strptime(start_date, '%Y-%m-%d')
    end_date = dt.strptime(end_date, '%Y-%m-%d')
 
    #применяем фильтрацию
    visits_filtered = dash_visits.query('item_topic in @selected_item_topics and \
									    dt >= @start_date and dt <= @end_date and \
									    age_segment in @selected_ages') 
   
    engagement_filtered = dash_engagement.query('item_topic in @selected_item_topics and \
											    dt >= @start_date and dt <= @end_date and \
											    age_segment in @selected_ages')
   
    #данные для графиков
    visits_by_item_topic = (visits_filtered.groupby(['item_topic','dt'])
                                           .agg({'visits' : 'sum'})
                                           .sort_values(by = 'visits', ascending = False)
                                           .reset_index())
                               
    visits_by_source_topic = (visits_filtered.groupby('source_topic')
                                             .agg({'visits' : 'sum'})
                                             .reset_index())
 
    engagement = (engagement_filtered.groupby('event')
                                     .agg({'unique_users' : 'mean'})
                                     .rename(columns = {'unique_users' : 'avg_unique_users'})
                                     .sort_values(by = 'avg_unique_users', ascending = False)
                                     .reset_index())

    engagement['avg_unique_users'] = (engagement['avg_unique_users'] / engagement['avg_unique_users'].max()).round(2)


    data_item_topic_dropdown = []
   
    #график истории событий по темам карточек
    for item in visits_by_item_topic['item_topic'].unique():
        current = visits_by_item_topic[visits_by_item_topic['item_topic'] == item]
        data_item_topic_dropdown += [go.Scatter(x = current['dt'],
                                   y = current['visits'],
                                   mode = 'lines',
                                   stackgroup = 'one',
                                   name = item)
                        ]
                       
    #график разбивки событий по темам источников
    data_pie_source_topic = [go.Pie(labels = visits_by_source_topic['source_topic'],
                                    values = visits_by_source_topic['visits'])
 
                        ]
                         
    #график средней глубины взаимодействия
    data_engagement_graph = [go.Bar(x = engagement['event'],
                                   y = engagement['avg_unique_users'],
                                   name = 'Среднее количество пользователей')
                        ]
   
   
 
    return (
            #график item-topic
            {
                'data': data_item_topic_dropdown,
                'layout': go.Layout(xaxis = {'title': 'Время'},
                                    yaxis = {'title': 'Количество событий'})
            },    
           
            #график pie-visits
            {
                'data': data_pie_source_topic,
                'layout': go.Layout()
            },
           
            #график engagement-graph
            {
                'data': data_engagement_graph,
                'layout': go.Layout(xaxis = {'title': 'Тип cобытия'},
                                    yaxis = {'title': 'Среднее число пользователей'},
                                    hovermode = 'closest')
             },
         )            
 
if __name__ == '__main__':
    app.run_server(debug=True)