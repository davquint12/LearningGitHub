import pandas as pd
import plotly.express as px
import pyodbc
import urllib
from sqlalchemy import create_engine
import streamlit as st

def null_values(df):
    df=df.isnull().sum()
    df = df.reset_index()
    df.columns = ['variable', 'count' ]
    df = df.sort_values(by=['count'], ascending=False)
    return df

def null_percent(df):
    df=(round((df.isnull().sum()/(len(df)))*100,2))
    df = df.reset_index()
    df.columns = ['variable', 'null_percent' ]
    df["null_percent"] = df["null_percent"].apply(lambda x : str(int(round(x, 0)))+" %")
    df = df.sort_values(by=['null_percent'], ascending=False)
    return df


def var_type(df):
    categorical_columns = pd.DataFrame(df.select_dtypes(include=['object']).columns.tolist(), columns=["Variable"]).T
    numerical_columns = pd.DataFrame(df.select_dtypes(exclude=['object']).columns.tolist(), columns=["Variable"]).T
    return [categorical_columns, numerical_columns]


def var_overview(df, column):
    unicos = df[column].nunique()
    nulls = df[column].isnull().sum()

    data_cant = df[column].count()

    per_nulls = round(df[column].isnull().sum()/(len(df[column]))*100,2)

    columna_memoria = df[column].memory_usage(deep=True)
    columna_memoria_kb = columna_memoria / 1024
    columna_memoria_kb = round(columna_memoria_kb,1)

    df_var_over = pd.DataFrame({"Parameter":["Total Data", "Distinct", "Missing","Missing (%)","Memory size (KiB)"], "Value": [data_cant, unicos, nulls, per_nulls, columna_memoria_kb ]} )

    df_var_over.index =df_var_over["Parameter"]
    df_var_over = df_var_over["Value"]
    return df_var_over

def generate_chart(df, col):
    # Count the number of penguins by species
    var_count = df[col].value_counts().reset_index()
    # Rename the columns
    var_count.columns = [col, 'Count']
    # Create the plot
    fig = px.bar(var_count, x='Count', y=col, orientation='h')

    # Adjust the size of the plot (width and height in pixels)
    fig.update_layout(width=200, height=260)

    # Remove axis titles and x-axis tick labels
    fig.update_xaxes(showticklabels=False, title=None)
    fig.update_yaxes(title=None)
    return fig


import sqlite3 # IMPORTAMOS LIBRERIA SQLIT3	

def conectsqlite(path):
    conn = sqlite3.connect(path) # GENERAMOS coneccion con la libreria y un proyecto particular
    cur = conn.cursor()  # este comando permite ejecutar comandos de sqlite 
    print("conectado")
    return conn , cur

def get_tables(cur):
        # Ejecutar consulta para obtener los nombres de las tablas
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    # Recuperar los nombres de las tablas
    tablas = [nombre[0] for nombre in cur.fetchall()]
    return tablas


def get_query(cur, query):
    cur.execute(query)  # este comando destruye la tabla si ya existe
    response = cur.fetchall() 
    return response

@st.cache_data
def conectsqlserver(direccion_servidor,nombre_bd,nombre_usuario,password ):
    try:
        # CONEXION ODBC
        conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                                direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
        print("conexion ODBC ok...")

        # STRING ENGINE PARA SQLKERNEL JUPYTER
        quoted = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +direccion_servidor+';DATABASE='+nombre_bd+';UID='+nombre_usuario+';PWD=' + password)
        engine_str = f'mssql+pyodbc:///?odbc_connect={quoted}'
        print("STRING ENGINE ok...")

        #ENGINE PARA PANDAS
        engine = create_engine(engine_str, echo=False, connect_args={'autocommit': True}, fast_executemany=True)
        print("Pandas engine OK...")

        cur = conexion.cursor()  # este comando permite ejecutar comandos de sqlite 
        print("conectado")
    
        print("OK! conexión exitosa")
        return engine, conexion, cur
    except Exception as e:
        # Atrapar error
        print("Ocurrió un error al conectar a SQL Server: ", e)


def get_tables_odbc_sqlserver(cur, schema):
        # Ejecutar consulta para obtener los nombres de las tablas
    cur.execute(f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {schema};")
    # Recuperar los nombres de las tablas
    tablas = [nombre[0] for nombre in cur.fetchall()]
    return tablas
