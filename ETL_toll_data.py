from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

#defining DAG arguments
default_args ={
    'owner': 'Luis Guerra',
    'start_date': days_ago(0),
    'email': ['gluisenriq9@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final AssignmentG',
    schedule_interval=timedelta(days=1),  
)
# First task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf tolldata.tgz -C home/project/airflow/dags/finalassignment/staging',
    dag=dag,
)
# Second Task
extract_data_from_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 /home/project/airflow/dags/staging/vehicle-data.csv\
                  > /home/project/airflow/dags/staging/csv_data.csv',
    dag=dag,
)
# Third Task
extract_data_from_tsv = BashOperator(
    task_id= 'extract_data_from_tsv',
    bash_command= 'cut -f5-7 < /home/project/airflow/dags/tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv',
    dag= dag,
)
# Fourth Task
extract_data_from_fixed_file = BashOperator(
    task_id= 'extract_data_from_fixed_file',
    bash_command= 'cut -c 59-68 < /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv',
    dag= dag,
)
# Fifth Task
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste /home/project/airflow/dags/staging/csv_data.csv\
                    /home/project/airflow/dags/staging/tsv_data.csv\
                    /home/project/airflow/dags/staging/fixed_width_data.csv\
                    > /home/project/airflow/dags/staging/extracted_data.csv',
    dag=dag,
)
# Sixth Task
Transform_and_load_data = BashOperator(
    task_id= 'Transform_and_load_data',
    bash_command= 'tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted_data.csv',
    dag= dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_file >> consolidate_data >> Transform_and_load_data