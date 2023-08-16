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


with DAG(dag_id='desglose-cobranza',
         default_args=default_args,
         schedule_interval=None) as dag:

    desglose_cobranza_task = SparkSubmitOperator(
                    task_id='desglose_cobranza_task',
                    conn_id='spark_default',
                    driver_memory='4g',
                    executor_memory='11g',
                    java_class='mx.com.findep.spark.desglose.cobranza.CaDesgloseCobranzaSpark',
                    application='/data/ca-desglose-cobranza-spark-1.0.1.jar',
                    application_args=[user, password, url ]
                    )
    

desglose_cobranza_task