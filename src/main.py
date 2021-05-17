# -*- coding: utf-8 -*-

# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

with open('/home/vof/Desktop/Projects/prouniProject/src/config.json', 'r') as f:
    config = json.load(f)


def getData(spark, user, password, database, qryStr):
    jdbcDF = spark.read.format('jdbc')\
        .option('url', f"jdbc:sqlserver://localhost:1433;databaseName={database}")\
        .option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .option('dbtable', qryStr)\
        .option("user", user) \
        .option("password", password) \
        .load()
    return jdbcDF


conf = SparkConf().setMaster("local").setAppName(config['db']['database'])
conf.set('spark.driver.extraClassPath',
         config['db']['jdbcDrivePath'])
conf.set('spark.executor.extraClassPath',
         config['db']['jdbcDrivePath'])
sc = SparkContext(conf=conf)
sc = SparkContext.getOrCreate()

sqlContext = SQLContext(sc)
sqlContext.sql(f"CREATE DATABASE IF NOT EXISTS {config['db']['database']}")
sqlContext.sql(f"USE {config['db']['database']}")

spark = sqlContext.sparkSession

qryStrAll = """ (
    SELECT *
    FROM todasBolsas
    ) t"""

allData = getData(spark, config['db']['user'], config['db']
                  ['password'], config['db']['database'], qryStrAll)
allData = allData.withColumn("AGE", floor(months_between(
    current_date(), col("DT_NASCIMENTO_BENEFICIARIO"))/lit(12)))

rangeAge = sqlContext.createDataFrame([(0, 24, '0-24'), (25, 30, '25-30'), (31, 40,
                                                                            '31-40'), (41, 50, '41-50'), (51, 9999, '51+')], ['age_f', 'age_to', 'RANGE_AGE'])

allData = allData.join(rangeAge, (allData.AGE >= rangeAge.age_f) & (
    allData.AGE <= rangeAge.age_to), 'inner')
allGrouped = allData.groupBy("MODALIDADE_ENSINO_BOLSA", "NOME_TURNO_CURSO_BOLSA", "ANO_CONCESSAO_BOLSA", "BENEFICIARIO_DEFICIENTE_FISICO", "SEXO_BENEFICIARIO_BOLSA",
                             "REGIAO_BENEFICIARIO_BOLSA", "RACA_BENEFICIARIO_BOLSA", 'SIGLA_UF_BENEFICIARIO_BOLSA', 'RANGE_AGE').count()
allDf = allGrouped.toPandas()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "ProUni Dashboard: Conhecendo as bolsas do ProUni!"
options = [
    {"label": "Raca", "value": "RACA_BENEFICIARIO_BOLSA"},
    {"label": "Sexo", "value": "SEXO_BENEFICIARIO_BOLSA"},
    {"label": "Deficiente", "value": "BENEFICIARIO_DEFICIENTE_FISICO"},
    {"label": "Turno", "value": "NOME_TURNO_CURSO_BOLSA"},
    {"label": "Modalidade", "value": "MODALIDADE_ENSINO_BOLSA"},
    {"label": "Idade", "value": "RANGE_AGE"},
]
MIN_YR = 2016
MAX_YR = 2018

app.layout = html.Div([
    html.Div(
        children=[
            html.P(children="📚", className="header-emoji"),
            html.H1(
                children="ProUni Dashboard", className="header-title"
            ),
            html.P(
                children="Analise das Bolsas do ProUni"
                " utilizando os dados coletados no Portal Brasileiro de Dados Abertos(https://dados.gov.br/)"
                " no período de 2016 a 2018",
                className="header-description",
            ),
        ],
        className="header",
    ),
    html.Div(
        children=[
            html.Div(
                [
                    html.Div(children="Ano", className="menu-title"),
                    dcc.RangeSlider(
                        id="date_slider",
                        min=MIN_YR,
                        max=MAX_YR,
                        count=1,
                        step=1,
                        value=[MIN_YR, MAX_YR],
                        marks={yr: str(yr)
                               for yr in range(MIN_YR, MAX_YR + 1)},
                        allowCross=False
                    )
                ],
                style={"width": "70%"},
            ),
            html.Div([
                html.Div(children="Variavel X", className="menu-title"),
                dcc.Dropdown(
                    id="dropdownX",
                    options=options,
                    value=options[0]['value'],
                    clearable=False,
                )
            ]),
            html.Div([
                html.Div(children="Variavel Y", className="menu-title"),
                dcc.Dropdown(
                    id="dropdownY",
                    options=options,
                    value=options[1]['value'],
                    clearable=False,
                )
            ])
        ], className="menu"),
    html.Div([html.Div(children="Grafico em barra dividido por região", className="menu-title"),
              dcc.Graph(id="bar-chart")]),
    dcc.Graph(
        id="line-chart", config={"displayModeBar": False},
    )
])


@ app.callback(
    [Output("bar-chart", "figure"), Output('line-chart', 'figure')],
    [Input("dropdownX", "value"), Input("dropdownY", "value"), Input("date_slider", "value")])
def update_bar_chart(optionX, optionY, data):
    mask = (allDf['ANO_CONCESSAO_BOLSA'] >= data[0]) & (
        allDf['ANO_CONCESSAO_BOLSA'] <= data[1])

    result = allDf[mask].groupby([optionY,
                                  'REGIAO_BENEFICIARIO_BOLSA', optionX]).sum().reset_index()
    bar = px.bar(result, x=optionY, y="count", hover_data=['REGIAO_BENEFICIARIO_BOLSA'],
                 color=optionX, barmode="group")
    resultLine = allDf.groupby(
        ['ANO_CONCESSAO_BOLSA', optionX]).sum().reset_index()

    line = px.line(resultLine, x="ANO_CONCESSAO_BOLSA", y="count",
                   color=optionX)
    return bar, line


app.run_server(debug=True)
