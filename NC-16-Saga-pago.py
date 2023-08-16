import json
from datetime import datetime, timedelta

import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'FINDEP',
    'start_date': datetime(year=2021, month=12, day=15, hour=12, minute=00, second=00),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


def saga_pago_function(ds, **kwargs):

    operacion = 'SAGA-PAGO'

    print('Conf: \n' + json.dumps(kwargs['dag_run'].conf, indent=4))

    # Info. error pago
    fase = kwargs['dag_run'].conf['fase']
    errMsg = kwargs['dag_run'].conf['errMsg']

    # Pago
    idBitacora = kwargs['dag_run'].conf['idBitacora']
    idDisposicion = kwargs['dag_run'].conf['idDisposicion']

    fechaValor = str(kwargs['dag_run'].conf['fechaValor'])

    idTransaccionAplicaReverso = kwargs['dag_run'].conf['idTransaccion']
    referencia = kwargs['dag_run'].conf['referencia']

    hasError = False

    inputMovimientos = {
        'idTransaccion': idTransaccionAplicaReverso,
        'fechaValor': fechaValor
    }

    urlMovimientos = 'http://cr-movimientos-service-v1-stable.new-core.svc.cluster.local/v1/movimientos/cancelacion/'
    responseMovimientos = None
    statusCode = None
    responseMovimientosJSON = None

    if fase in ['LEDGER', 'SALDOS', 'FONDO']:
        try:
            print('Llamando servicio Ledger')

            responseMovimientos = requests.post(url=urlMovimientos, json=inputMovimientos)
            statusCode = responseMovimientos.status_code
            responseMovimientosJSON = responseMovimientos.json()

            print('URL: {}\nRequest: {}\nEstatus: {}\nResponse: {}'.format(urlMovimientos, inputMovimientos, str(statusCode), json.dumps(responseMovimientosJSON)))
        except:
            statusCode = 500
            hasError = True

    urlPagos = 'http://ca-pagos-service-v1-stable.new-core.svc.cluster.local/v1/pagos/referencia/{}'.format(idTransaccionAplicaReverso)
    responsePagos = None
    statusCode = None

    if ('MOVIMIENTOS' == fase or fase == 'LEDGER' or fase == 'SALDOS') and not hasError:
        try:
            print('Llamando servicio pagos')
            responsePagos = requests.delete(url=urlPagos)
            statusCode = responsePagos.status_code
            print('URL: {}, Estatus: {}'.format(urlPagos, str(statusCode)))
        except:
            statusCode = 500
            hasError = True

    estatusKafka = None

    urlKafka = 'http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core'

    inputKafka = None

    if not hasError:
        try:
            print('Llamando Kafka: Exito')

            inputKafka = {
                'status': 'S',
                'mensaje': 'Saga aplicada | Hubo un error al aplicar pago en el servicio {}'.format(errMsg),
                'idBitacora': idBitacora,
                'operacion': operacion
            }

            requestKafka = requests.post(url=urlKafka, json=inputKafka)
            estatusKafka = requestKafka.status_code

            print('URL: {}, Request:{}, Estatus: {}'.format(urlKafka, inputKafka, str(estatusKafka)))
        except:
            print('Error llamando topico')
    else:
        try:
            print('Llamando Kafka: Error Saga')

            inputKafka = {
                'status': 'F',
                'mensaje': 'Saga no aplicada | Hubo un error al aplicar pago en el servicio {}'.format(errMsg),
                'idBitacora': kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='idBitacora'),
                'operacion': operacion
            }

            requestKafka = requests.post(url=urlKafka, json=inputKafka)
            estatusKafka = requestKafka.status_code

            print('URL: {}, Request:{}, Estatus: {}'.format(urlKafka, inputKafka, str(estatusKafka)))
        except:
            print('Error llamando topico')


with DAG(
    dag_id='NC-16-Saga-pago',
    default_args=default_args,
    schedule_interval=None
) as dag:
    saga_pago_task = PythonOperator(
        task_id='saga_pago_task',
        provide_context=True,
        python_callable=saga_pago_function
    )

saga_pago_task
