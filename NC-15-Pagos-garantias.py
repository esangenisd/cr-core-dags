import json
from datetime import datetime


from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests

default_args = {
    'owner': 'Francisco Perez',
    'start_date': datetime(2021, 8, 1, 11, 0, 0)
}

def mensaje_start(ds, **kwargs):
    print('inicia flujo validación')
    idLineaCredito = kwargs['dag_run'].conf['idLineaCredito']
    montoGarantia = kwargs['dag_run'].conf['montoGarantia']
    idSolicitudGarantia = kwargs['dag_run'].conf['idSolicitudGarantia']
    idBitacoraPagosGarantias = kwargs['dag_run'].conf['idBitacoraPagosGarantias']
    kwargs['ti'].xcom_push(key='idLineaCredito', value=idLineaCredito)
    kwargs['ti'].xcom_push(key='montoGarantia', value=montoGarantia)
    kwargs['ti'].xcom_push(key='idSolicitudGarantia', value=idSolicitudGarantia)
    kwargs['ti'].xcom_push(key='idBitacoraPagosGarantias', value=idBitacoraPagosGarantias)

def mensaje_movimiento(ds, **kwargs):
    print('registra garantia pagada')
    idLineaCredito=kwargs['ti'].xcom_pull(task_ids='recibe_pago_garantia', key='idLineaCredito')
    montoGarantia=kwargs['ti'].xcom_pull(task_ids='recibe_pago_garantia', key='montoGarantia')
    idBitacoraPagosGarantias=kwargs['ti'].xcom_pull(task_ids='recibe_pago_garantia', key='idBitacoraPagosGarantias')
    dateTimeObj = datetime.now()
    timestampStr = dateTimeObj.strftime("%Y-%m-%dT%H:%M:%S")
    headers = {'content-type': 'application/json'}
    movimiento = {'comentarios': 'Pago garantia', 'fechaMovimiento': timestampStr, 'folio': '1234',
                  'idLineaCredito': idLineaCredito, 'monto': montoGarantia, 'status': 'ACTI',
                  'tipoMovimiento': 'DGDE', 'usuario': '045'}
    try:
        urlValidacion = requests.post('http://movimientos-garantias-service-v1-stable.new-core.svc.cluster.local/movimientosGarantias/', headers=headers, json=movimiento)
    except:
        bitacora = {
          "descripcion": "Error DAG garantias actualiza pagada",
          "detalle": "Error en el servicio de movimientos garantias",
          "idBitacoraPagosGarantias": idBitacoraPagosGarantias,
          "status": "ERRO"
        }
        registra_bitacora(bitacora)
        raise Exception("Error en registro garantia pagada")
    if urlValidacion.status_code!=201:
        bitacora = {
          "descripcion": "Error DAG garantias actualiza pagada",
          "detalle": urlValidacion.text,
          "idBitacoraPagosGarantias": idBitacoraPagosGarantias,
          "status": "ERRO"
        }
        registra_bitacora(bitacora)
        raise Exception("Error en registro garantia pagada")
    urlValidacionParse= urlValidacion.json()
    kwargs['ti'].xcom_push(key='movimiento', value=urlValidacionParse)
    print(urlValidacionParse)

def mensaje_ledger(ds, **kwargs):
    print('registra garantia ledger')

def mensaje_solicitud(ds, **kwargs):
    print('actualiza solicitud garantia')
    idSolicitudGarantia=kwargs['ti'].xcom_pull(task_ids='recibe_pago_garantia', key='idSolicitudGarantia')
    idBitacoraPagosGarantias=kwargs['ti'].xcom_pull(task_ids='recibe_pago_garantia', key='idBitacoraPagosGarantias')
    movimiento=kwargs['ti'].xcom_pull(task_ids='registra_movimiento_garantia', key='movimiento')
    headers = {'content-type': 'application/json'}
    actualiza = { 'idSolicitudGarantia': idSolicitudGarantia, 'status': 'COMP'}
    try:
        urlValidacion = requests.put('http://solicitud-garantias-service-v1-stable.new-core.svc.cluster.local/garantias/update', headers=headers, json=actualiza)
    except:
        urlDelete = requests.delete('http://movimientos-garantias-service-v1-stable.new-core.svc.cluster.local/movimientosGarantias/' + str(movimiento['idGarantiaMovimiento']))
        print(urlDelete)
        bitacora = {
          "descripcion": "Error DAG garantias actualiza pagada",
          "detalle": "Error en el servicio de solicitud garantias",
          "idBitacoraPagosGarantias": idBitacoraPagosGarantias,
          "status": "ERRO"
        }
        registra_bitacora(bitacora)
        raise Exception("Error en registro garantia pagada")
    if urlValidacion.status_code!=200:
        urlDelete = requests.delete('http://movimientos-garantias-service-v1-stable.new-core.svc.cluster.local/movimientosGarantias/' + str(movimiento['idGarantiaMovimiento']))
        print(urlDelete)
        bitacora = {
          "descripcion": "Error DAG garantias actualiza pagada",
          "detalle": urlValidacion.text,
          "idBitacoraPagosGarantias": idBitacoraPagosGarantias,
          "status": "ERRO"
        }
        registra_bitacora(bitacora)
        raise Exception("Error en actualiza solicitud garantia")
    urlValidacionParse= urlValidacion.json()
    print(urlValidacionParse)


def registra_bitacora(errorJson):
    print('bitacora')
    url= 'http://bitacora-pagos-garantia-service-v1-stable.new-core.svc.cluster.local/bitacoras/update/'
    headers = {'content-type': 'application/json'}
    result = requests.put(url, headers=headers, json=errorJson)
    print(result)
    return result





with DAG(
    'NC-15-Pagos-garantias',
         default_args=default_args,
         schedule_interval='@once') as dag:
    start = PythonOperator(task_id='recibe_pago_garantia',
                           python_callable=mensaje_start)

    registra_movimiento_garantia = PythonOperator(task_id='registra_movimiento_garantia',
                                                  python_callable=mensaje_movimiento)

    registra_garantia_ledger = PythonOperator(task_id='registra_garantia_ledger',
                                                  python_callable=mensaje_ledger)

    actualiza_solicitud_garantia = PythonOperator(task_id='actualiza_solicitud_garantia',
                                                  python_callable=mensaje_solicitud)

    fin = DummyOperator(task_id="publica_en_kafka")


start >> registra_movimiento_garantia >> registra_garantia_ledger >> actualiza_solicitud_garantia >> fin