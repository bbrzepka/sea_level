from prefect import task, flow
from upload_gcp import upload_aux_big_query, update_final_big_query
from sea_level_data import data_sea_level
from prefect.deployments import Deployment
from prefect.task_runners import SequentialTaskRunner
from params import *

@task
def webscrape_sea_data():
    df_data = data_sea_level()
    upload_aux_big_query(df_data)

@task
def update_sea_data():
    update_final_big_query()

@flow(name=PREFECT_FLOW_NAME)
def etl_flow():
    webscrape_sea_data()
    update_sea_data()

def deploy():
    deployment = Deployment.build_from_flow(
        flow=etl_flow,
        name="sea_level_deployment"
    )
    deployment.apply()


if __name__ == "__main__":
    deploy()
