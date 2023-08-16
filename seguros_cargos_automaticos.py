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



user = Variable.get("core_spark_user")
password = Variable.get("core_spark_pass")
url = Variable.get("core_spark_db_url")


with DAG(dag_id='seguros_cargos_automaticos',
         default_args=default_args,
         schedule_interval=None) as dag:

    seguros_cargos_automaticos_task = SparkSubmitOperator(
                    task_id='seguros_cargos_automaticos_task',
                    conn_id='spark_default',
                    driver_memory='10g',
                    name='seguros_cargos_automaticos',
                    java_class='com.mx.findep.seguros.cargos.automaticos.CargosAutomaticos',
                    application='/data/cr-seguros-cargos-automaticos-service-1.0.1.jar',
                    application_args=[ "tyson_spark_prod", "zf9Qd#0*zx-L46Vj", "jdbc:postgresql://10.108.224.22:5432/serv_digital_core_tyson?currentSchema=serv_digital_core"]
                    )
    
    
seguros_cargos_automaticos_task
