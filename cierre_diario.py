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


with DAG(dag_id='cierre-diario-operativo',
         default_args=default_args,
         schedule_interval=None) as dag:

    cierre_diario_task = SparkSubmitOperator(
                    task_id='cierre_diario_task',
                    conn_id='spark_default',
                    driver_memory='5g',
                    executor_memory='5g',
                    name='cierre_diario',
                    java_class='mx.com.findep.spark.cierre.diario.CierreDiarioCoreApplication',
                    application='/data/cierre-diario-operativo-2.1.4.jar',
                    application_args=[ user, password, shadow_db_url, "0", "30", "15" , "spark://spark-master-0.spark-master.spark.svc.cluster.local:7077" ]
                    )

cierre_diario_task 






