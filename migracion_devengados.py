import time
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'Tzompantzi',
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 10, 23, 0, 0),
}



user=Variable.get("core_spark_user")
password = Variable.get("core_spark_pass")
shadow_db_url=Variable.get("core_spark_db_url")
credprod_url=Variable.get("credprod_db_url")
credprod_user=Variable.get("spark_credprod_user")
credprod_pass=Variable.get("spark_credprod_pass")
kevin_url=Variable.get("kevin_db_url")
kevin_user=Variable.get("spark_kevin_user")
kevin_pass=Variable.get("spark_kevin_pass")


with DAG(dag_id='migracion_devengados',
         default_args=default_args,
         schedule_interval=None) as dag:

    migracion_devengados_task = SparkSubmitOperator(
                    task_id='migracion_devengados_task',
                    conn_id='spark_default',
                    driver_memory='4g',
                    executor_memory='4g',
                    java_class='com.mx.findep.devengados.migracion.Main',
                    application='/data/migracion-devengados-jar-1.5.0.jar',
                    conf={'spark.logConf': 'true', 
                          'spark.driver.cores': 4, 
                          'spark.sql.shuffle.partitions':150, 
                          'spark.executor.extraJavaOptions': '-XX:+UseG1GC', 
                          'spark.speculation': 'true',
                          'spark.executor.instances':47, 
                          'spark.rpc.numRetries':20,
                          'spark.task.maxFailures':20,
                          'spark.executor.cores':4},                    
                    application_args=[user, password, shadow_db_url]
                    )
    
    


    
migracion_devengados_task 
