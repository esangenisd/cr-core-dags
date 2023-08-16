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
	



with DAG(dag_id='migracion_contratos',
         default_args=default_args,
         schedule_interval=None) as dag:

    migracion_contratos = SparkSubmitOperator(
                    task_id='migracion_contratos',
                    conn_id='spark_default',
                    driver_memory='5g',
                    executor_memory='5g',                    
                    conf={'spark.driver.cores': 4 },
                    java_class='com.mx.findep.migracion.App',
                    application='/data/migracion-contratos-proyect-0.0.1-COMPLETO.jar',
                    application_args=[user, password, shadow_db_url,credprod_user, credprod_pass, credprod_url, kevin_user, kevin_pass, kevin_url, "REVOF,TRADF", "VENCIDO" , "ALL", "spark://spark-master-0.spark-master.spark.svc.cluster.local:7077"]

)
    
    
migracion_contratos
