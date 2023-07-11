from params import *
import pandas_gbq
from sea_level_data import data_sea_level

def upload_aux_big_query(df):
    """
    Upload a dataframe into Big Query auxiliar table
    """
    pandas_gbq.to_gbq(df, 'sea.sea_level_aux', project_id=GCP_PROJECT, if_exists='replace', table_schema=[{'name': 'sea_level', 'type': 'float'},{'name': 'date', 'type': 'string'}])

def update_final_big_query():
    """
    Update final table in Big Query using data from auxiliar table
    """
    sql = """
    SELECT Time as date, NivelRedmar as sea_level
    FROM sea.sea_level_aux
    WHERE datetime(Time) not in  (SELECT datetime(date) from sea.sea_level_bcn)
    """
    df = pandas_gbq.read_gbq(sql, project_id=GCP_PROJECT)
    print(df.info())
    pandas_gbq.to_gbq(df, 'sea.sea_level_bcn', project_id=GCP_PROJECT, if_exists='append',table_schema=[{'name': 'date', 'type': 'string'},{'name': 'sea_level', 'type': 'float'}])
