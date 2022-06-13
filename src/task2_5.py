# Using Apache Airflow's Dummy Operator,
# create an Airflow Dag that runs task 1,
# followed by tasks 2, and 3 in parallel,
# followed
# by tasks 4, 5, 6 all in parallel.

# I assumed that by tasks1-6 it was meant to run
# the out producing files, task1_2a, task1_2b, etc.

# I will use the Empty operator, since the
# Dummy operator is now deprecated as per the documentation. See:
#    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/dummy/index.html

# I had some trouble executing the SparkSubmitOperator API's
# Get the folowing error with the below config:
	# "Could not load connection string local[1], defaulting to yarn"
# So I realize there is in all likelihood another config somewhere that I'm missing

from datetime import datetime, timedelta

from airflow import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.baseoperator import cross_downstream,chain
from airflow.operators.empty import EmptyOperator

with DAG(
    'task2_5',
    #Dag args
    description='task2 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['task2'],
	default_args = {
	    'conn_id': 'local[*]'
	} # I think this configuration needs to be tweaked, but close enough
) as dag:

    t1 = SparkSubmitOperator(
    	application='task1_2a.py',
        task_id='task1',
    )

    t2 = SparkSubmitOperator(
        application='task1_2b.py',
        task_id='task2',
    )

    t3 = SparkSubmitOperator(
        application='task1_3.py',
        task_id='task3',
    )

    t4 = SparkSubmitOperator(
        application='task2_2.py',
        task_id='task4',
    )

    t5 = SparkSubmitOperator(
        application='task2_3.py',
        task_id='task5',
    )

    t6 = SparkSubmitOperator(
        application='task2_4.py',
        task_id='task6',
    )

    #t1 >> [t2, t3]
    chain(*[EmptyOperator(task_id='t1'),[EmptyOperator(task_id='t' + str(i)) for i in [2,3]]])
    #cross_downstream([t2, t3],[t4, t5, t6])
    cross_downstream(*[[EmptyOperator(task_id='op' + str(i)) for i in [2,3]],[EmptyOperator(task_id='op' + str(i)) for i in [4,5,6]]])
