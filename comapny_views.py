from airflow.sdk import DAG
import os
from pendulum import datetime
from airflow.providers.standard.operators.python import PythonOperator
from Airflow_assignment.extract import download
from Airflow_assignment.load import load_data
from Airflow_assignment.transform import transform_download
from airflow.providers.smtp.operators.smtp import EmailOperator
import os


with DAG(
    dag_id="company_views",
    start_date=datetime(2025, 10, 20),
    schedule="0 */1 * * *"
):
    def run_all_scripts():

        latest_file=download()


        processed_views=transform_download(latest_file)


        load_data(processed_views)

    run_it_all=PythonOperator(
        task_id="run_all_scrips",
        python_callable=run_all_scripts,
    )

    send_notification = EmailOperator(
        task_id="send_notifcation",
        to=["mr.chikeze@gmail.com"],
        subject="Log Reports for Company Online Views {{ ds }}",
        html_content="""
        <h3>Launches Update</h3>
        <p>Hi Chika, the airflow pipeline ran successfully</p>
        

        <p>Warm regards, pipeline_bot</p>
        """, conn_id="smtp_conn"
    )

    run_it_all >> send_notification