#Using Apache Airflow's Dummy Operator, 
#create an Airflow Dag that runs task 1, 
#followed by tasks 2, and 3 in parallel, 
#followed
#by tasks 4, 5, 6 all in parallel. 

#I will use the Empty operator, since the 
#Dummy operator is now deprecated as per the documentation. See:
#    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/dummy/index.html

#classairflow.operators.empty.EmptyOperator

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator