

from locale import normalize
# from dotenv import load_dotenv, find_dotenv
import os
# import matplotlib
# from networkx import dfs_edges
import streamlit as st
import pandas as pd
# from pandasai import PandasAI
# from pandasai.llm.openai import OpenAI
# import matplotlib
from PIL import Image
from functions import conectsqlite, conectsqlserver, generate_chart, get_query, get_tables, get_tables_odbc_sqlserver, null_percent, null_values, var_overview, var_type
import plotly.express as px
# import pygwalker as pyg
# import streamlit.components.v1 as components
import datetime

st.set_page_config(page_title="EDA-TOOL", page_icon=":panda_face:", layout="wide",initial_sidebar_state="expanded",)


load_dotenv(find_dotenv())


header = st.container()

header.title(f":panda_face: EDA-Tool Prompt-driven Analysis")
st.write("---")

header.write("""<div class='fixed-header'/>""", unsafe_allow_html=True)
### Custom CSS for the sticky header
st.markdown(
    """
<style>
    div[data-testid="stVerticalBlock"] div:has(div.fixed-header) {
        position: sticky;
        top: 2.8rem;
        z-index: 999;
    }
    .fixed-header {
        background-color: white;
        color: #b8c2ff;
        border-bottom: 5px solid #b8c2ff;
    }
</style>

    """,
    unsafe_allow_html=True
)



# SIDEBAR
with st.sidebar:

    # inicializando el session state
    if "documents" not in st.session_state:
        st.session_state.documents = {}

    if "credentials" not in st.session_state:
        st.session_state.credentials = {}

    if "conection" not in st.session_state:
        st.session_state.conection = {}

    # # resetea el estado cada ves qeu se llama esta funcion
    # def clear_state():
    #     st.session_state.documents = {}

    status = False

    # # # ***********IMAGEN TITULO *********************

    path = r"C:\Users\davquint1\OneDrive - Publicis Groupe\Desktop\pandasai\image2.png"

    image = Image.open(path)
    st.image(image)

# #############################################################
# #############################################################
# HEADER SECCION **************************
    st.header("Activate BOT")

    with st.expander("Credentials"):

        with st.container():
            # --------------------------
            st.subheader("OpenIA credentials")
            openia_ = st.text_input("OpenAI key",placeholder= "sk-1..",type='password')

            col5, col6 = st.columns([0.5, 0.4])
            col5.empty()
            if col6.button("Activate Bot",key="citybotactivate"):
                # download data
                with st.container():
                    if (len(openia_) == 51) & ( openia_.startswith("sk")):
                        st.session_state.credentials = {"openia_": openia_,
                                                        "activate": ":white_check_mark: Bot is Activate, key Uploaded"}
                        openia_ = st.session_state.credentials["openia_"]
                    elif (len(openia_) == 0):
                        st.write(":x: Error! Insert a valid credentials")
                    else:
                        st.write(":x: Error! Insert a valid credentials")

        if "activate" not in  st.session_state.credentials:
            st.write(":cry: Insert a validate key to active the bot")
        else:
            st.write(st.session_state.credentials["activate"])
        # st.write(st.session_state.credentials)

    st.header("Data to Analyse")

    with st.expander("Upload Dataset"):
        option = st.selectbox("Select data input method",
                     ("Upload .csv", "Conect to SQLite", "Conect to SQL server"))

        if option == "Upload .csv":
            uploaded_file = st.file_uploader("upload", type=["csv"], accept_multiple_files=False, label_visibility="hidden")

            # uploaded_file = st.file_uploader("upload", type=["csv"], accept_multiple_files=False, label_visibility="hidden", on_change=clear_state)

            if uploaded_file is not None:
                with st.spinner("Uploading..."):
                    filename = uploaded_file.name   
                    df = pd.read_csv(uploaded_file)
                    # impide que se sobrescriba la data en el session
                    # misma clave en un dic no es permitda
                    st.session_state.documents[filename] = df
                    st.write(":white_check_mark: File uploaded: ")
                    st.write(f"Filename: {filename}")
                    # st.write(df.head(3))
            else:
                # si no se carga nada vacia el session state
                print("empty")
                # clear_state()
                st.warning("Upload a .csv file ")

            st.write(st.session_state.documents.keys())
        elif option == "Conect to SQLite":
            st.subheader("SQLite conection")
            sqlite = st.text_input("Database path", placeholder= "c:/..")
            if st.button("Conect",key="sqlite conn"):
                with st.spinner("Conecting..."):
                    st.session_state.conection = {"path": sqlite,
                                                    "conexion": '',
                                                        "cursor": '',
                                                        "tables": '',
                                                        "status": ''
                                                            }
                    st.success( ":white_check_mark: conected to database")
            if "path" in st.session_state.conection:
                try:
                    conn, cur = conectsqlite(sqlite)
                    tables = get_tables(cur)
                    status = True
                    st.session_state.conection["conexion"] = conn,
                    st.session_state.conection["cursor"] = cur,
                    st.session_state.conection["tables"] = tables,
                    st.session_state.conection["status"] = status,
                except:
                    st.error(":x: Error! can not conect to database")
                option = st.selectbox(
                "Select Table to fetch data",st.session_state.conection["tables"][0] )

                query = f"SELECT COUNT(*) FROM {option};"

                response = get_query(st.session_state.conection['cursor'][0], query )
                response = int(response[0][0])

                st.info(f"Total Data: {response}")

                amount_data = st.slider('How much data do you want?', 1, response, 100)

                amount_data = st.number_input('How much data do you want?', min_value=1, max_value=response ,step=100)

                st.info(f"Rows selected: {amount_data}")

                query = f"SELECT * FROM {option} LIMIT {amount_data};"

                if st.button("fetch data",key="fetch"):
                    with st.spinner("fetching data..."):
                        try:
                            query = f"SELECT * FROM {option} LIMIT {amount_data};"
                            data = get_query(st.session_state.conection['cursor'][0], query)

                            query2 = f"PRAGMA table_info({option});"
                            columns = get_query(st.session_state.conection['cursor'][0], query2)

                            colu = []
                            for item in columns:
                                colu.append(item[1])
                            
                            st.write(f"Table: {option}")

                            df = pd.DataFrame(data, columns=colu)
                            st.session_state.documents[option] = df
                            st.success(":white_check_mark: table uploaded ")
                        except:
                            st.error(":x: Error! no se peude convertir")
            # st.session_state.conection
        else:
            st.subheader("SQL Server")
            user = st.text_input("Username", placeholder= "user")
            pas = st.text_input("Password", placeholder= "pass", type='password')
            server = st.text_input("Server name", placeholder= "server")
            dbname = st.text_input("Database name", placeholder= "db")
            schema = st.text_input("Database Schema", placeholder= "Schema")

            if st.button("Conect",key="sqlserver conn"):
                with st.spinner("Conecting..."):
                    st.session_state.conection = {"Username": user,
                                                    "Password": pas,
                                                        "Server": server,
                                                        "dbname": dbname,
                                                        "Schema":schema,
                                                        "tables": "",
                                                        "conexion": "",
                                                        "engine": "",
                                                        "status": ""
                                                            }
                    st.success( ":white_check_mark: conected to database")
            if "Password" in st.session_state.conection:
                try:
                    engine_pandas, conodbc, cur = conectsqlserver(server, dbname, user, pas )
                    tables = get_tables_odbc_sqlserver(cur, schema)
                    status = True
                    st.session_state.conection["conexion"] = conodbc,
                    st.session_state.conection["engine"] = engine_pandas
                    st.session_state.conection["cursor"] = cur,
                    st.session_state.conection["tables"] = tables,
                    st.session_state.conection["status"] = status,
                    st.session_state.conection
                    print("conection ok")
                except:
                     st.error(":x: Error! can not conect to database")

                option = st.selectbox(
                "Select Table to fetch data",st.session_state.conection["tables"][0])

                query = f"SELECT COUNT(*) FROM {schema}.{option};"

                response = get_query(st.session_state.conection['cursor'][0], query )
                response = int(response[0][0])

                st.info(f"Total Data: {response}")

                amount_data = st.number_input('How much data do you want?', min_value=1, max_value=response ,step=100)

                st.info(f"Rows selected: {amount_data}")

                query = f"SELECT * FROM {schema}.{option} LIMIT {amount_data};"

                if st.button("fetch data",key="fetch"):
                    with st.spinner("fetching data..."):
                        try:
                            # enviar query con pandas y convertir en df
                            query = f"SELECT TOP {amount_data} * FROM {schema}.{option};"
                            df = pd.read_sql(query, con=st.session_state.conection["engine"])
                            st.session_state.documents[option] = df
                            st.success(":white_check_mark: table uploaded ")
                        except:
                            st.error(":x: Error! no se puede convertir")
            # st.session_state.conection


    with st.expander("Enter Question"):
        llm = OpenAI(api_token=openia_)

        pandas_ai = PandasAI(llm)

        prompt = st.text_area('Enter your prompt:')
        if st.button("Generate"):
            if prompt:
                with st.spinner("Generating response..."):
                    st.write("Response:")
                    response = pandas_ai.run(df, prompt=prompt)
                    code = pandas_ai.last_code_generated
                    st.write(response)
                    st.write("Code:")
                    st.code(code, language="python")
                    # pandas_ai.run(df, prompt=prompt, show_code=True )
                    # st.code(code, language="python")
            else:
                st.warning("Please enter your prompt.")


# C:\Users\fermina\Box Sync\Laptop_Fer_BU\PROGRMACION\PYTHON\big_data\project_bigdata_DB


# #############################################################
# footer
    path = r"C:\Users\davquint1\OneDrive - Publicis Groupe\Desktop\pandasai\image2.png"

    st.write("")
    col7, col8, col9 = st.columns([1,2,1])
    col7.empty()
    col9.empty()
    image = Image.open(path)
    col8.image(image, width= 100)

    st.write("""Created by Fernado Mina | PGD 2023""")

# #############################################################

with st.container():
    if len(list(st.session_state.documents.keys())) != 0:
        st.header("Preview data")
        key =  list(st.session_state.documents.keys())[0]
        df = st.session_state.documents[key]
        st.write(df.head())

        with st.expander("Discribe Dataset"):
            # visualizacion del df
            tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Describe", "Overview values", "Variables Type", "Varaibles Correlation", "unique values","variable Overview"])
            with tab1:
                # describe
                st.subheader("Dataset stadistics")
                st.write(df.describe().T)

            with tab2:    
                cola, colb, colc = st.columns([1,1,1])
                with cola:
                    st.subheader("Unique values")
                    st.write(df.nunique())
                with colb:
                    st.subheader("Count of Null values")
                    st.write(null_values(df))
                with colc:
                    st.subheader("Percentage of Null values")
                    st.write(null_percent(df))
            with tab3:
                type_var = var_type(df)
                st.subheader("Categorical")
                st.write(type_var[0])
                st.subheader("Numerical")
                st.write(type_var[1])
            with tab4:
                pass
            with tab5:
                st.subheader("Unique values by columns")
                categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
                option = st.selectbox(
                        'select column',
                        categorical_columns)
                st.write(df[option].unique())
            with tab6:
                st.subheader("Variable Description")
                categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
                option = st.selectbox(
                        'select categorical variable',
                        categorical_columns)

                cole, colf = st.columns(2)
                with cole:
                    st.write(var_overview(df,option))
                with colf:
                    fig = generate_chart(df,option)
                    st.plotly_chart(fig, theme="streamlit",use_container_width=True)


        st.header("Data Viisualization")
        with st.container():
            # Genera el HTML usando Pygwalker
            pyg_html = pyg.walk(df, return_html=True)
            
            # Inserta el HTML en la aplicación de Streamlit
            components.html(pyg_html, height=1000, scrolling=True)
    else:
        st.warning("Upload a file to create a EDA report")
        df = None





