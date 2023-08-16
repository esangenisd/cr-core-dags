import json
from datetime import datetime, timedelta
from distutils.util import strtobool

import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'FINDEP',
    'start_date': datetime(year=2021, month=12, day=15, hour=12, minute=00, second=00),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


def saga_reverso_function(ds, **kwargs):

    operacion = 'SAGA-REVERSO'

    print('Conf: \n' + json.dumps(kwargs['dag_run'].conf, indent=4))

    idBitacoraPago = kwargs['dag_run'].conf['idBitacoraPago']
    referencia = kwargs['dag_run'].conf['referencia']
    referenciaAplicaReverso = kwargs['dag_run'].conf['referenciaAplicaReverso']
    cancelacionHasError = bool(strtobool(kwargs['dag_run'].conf['cancelacionHasError']))
    fase = kwargs['dag_run'].conf['fase']
    fechaValor = str(kwargs['dag_run'].conf['fechaValor'])

    hasError = False
    faseSaga = None

    urlBitacora = 'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/pagos/bitacorapagos/{}'.format(idBitacoraPago)

    statusBitacora = None
    responseBitacoraJSON = None

    if cancelacionHasError:
        try:
            responseBitacora = requests.get(url=urlBitacora)
            statusBitacora = responseBitacora.status_code
            print('URL: {}, Estatus: {}'.format(urlBitacora, str(statusBitacora)))
            responseBitacoraJSON = responseBitacora.json()
            print('Response: {}'.format(responseBitacoraJSON))
        except:
            statusBitacora = 500
            hasError = True
            faseSaga = 'consulta bitacora'

        peticionDag = json.loads(responseBitacoraJSON['peticionDag'])

        ledgerPagoJSON = peticionDag['conf']['pago']['infoPagoContrato']['pagoAplicaLedger']
        ledgerBonifJSON = peticionDag['conf']['bonificacion']['bonificacionAplicaLedger']

        idDisposicion = responseBitacoraJSON['idDisposicion']
        contrato = responseBitacoraJSON['contrato']
        tipoOperacion = responseBitacoraJSON['operacion']
        fechaValor = datetime.strptime(str(responseBitacoraJSON['fechaValor']), '%Y-%m-%d')
        fechaLimite = fechaValor.today() + timedelta(days=1)
        medioPago = responseBitacoraJSON['medioPago']
        montoPago = responseBitacoraJSON['monto']
        bonificacionComision = responseBitacoraJSON['bonificacionComision']
        liquidado = responseBitacoraJSON['liquida']

        bonificacionComisionJSON = {
            'contrato': idDisposicion,
            'fechaValor': datetime.strftime(fechaValor, '%Y-%m-%d'),
            'fechaLimite': fechaLimite,
            'empresa': 'CORE',
            'sentido': 'CONTRATO',
            'medioPago': medioPago,
            'montoPago': montoPago,
            'objetivo': 'COMISION'
        }

        print('Bonificacion Comision: {}'.format(bonificacionComisionJSON))
        print('Ledger pago: {}'.format(ledgerPagoJSON))
        print('Ledger bonificacion: {}'.format(ledgerBonifJSON))

    if cancelacionHasError and not hasError and fase in ['MOVIMIENTOS', 'PAGOS', 'SALDOS']:

        inputLedger = {}
        urlLedger = 'http://cr-ledger-service-v1-stable.new-core.svc.cluster.local/v1/ledger/aplicarOperaciones'

        ledgerPagos = json.loads(kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', value='ledgerPagoJSON'))
        ledgerBonifs = json.loads(kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', value='ledgerBonifJSON'))

        for ledgerPago in ledgerPagos:
            ledgerPago['fechaValor'] = kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='fechaValor')
            ledgerPago['referencia'] = kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='referencia')

        if ledgerBonifs != None:
            for ledgerBonif in ledgerBonifs:
                ledgerBonif['fechaValor'] = kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='fechaValor')
                ledgerBonif['referencia'] = kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='referencia')

        if ledgerBonif != None:
            inputLedger = {
                ledgerPagos,
                ledgerBonifs
            }

        else:
            inputLedger = ledgerPagos

        try:
            responseLedger = requests.post(url=urlLedger, json=inputLedger)
            statusCodeLedger = responseLedger.status_code
            responseLedgerJSON = responseLedger.json()
            print('URL: {}\nRequest: {}\nEstatus: {}\nResponse: {}'.format(urlLedger, inputLedger, str(statusCodeLedger), responseLedgerJSON))
        except:
            hasError = True
            faseSaga = 'LEDGER'

    urlGetMovimientos = 'http://cr-movimientos-service-v1-stable.new-core.svc.cluster.local/v1/movimientos/search?idDisposicion={}&idTransaccion={}&size=500'.format(idDisposicion, idTransaccion)
    urlPostMovimientos = 'http://cr-movimientos-service-v1-stable.new-core.svc.cluster.local/v1/movimientos/'

    movimientosPago = []

    responseGetMovimientos = None
    statusCodeGet = None
    responseGetMovimientosJSON = None

    responsePostMovimientos = None
    statusCodePost = None
    responsePostMovimientosJSON = None

    if cancelacionHasError and not hasError and fase in ['PAGOS', 'SALDOS']:
        try:
            responseGetMovimientos = requests.get(url=urlGetMovimientos)
            statusCodeGet = responseGetMovimientos.status_code
            print('URL: {}\nEstatus: {}'.format(urlGetMovimientos, str(statusCodeGet)))

            if statusCodeGet == 200:
                responseGetMovimientosJSON = responseGetMovimientos.json()
                print('Response: {}'.format(responseGetMovimientosJSON))

                movimientos = responseGetMovimientosJSON['content']
                for movimiento in movimientos:
                    movimientosPago.append({
                        'idDisposicion': movimiento['idDisposicion'],
                        'contrato': movimiento['contrato'],
                        'claveEmpresa': movimiento['claveEmpresa'],
                        'idTransaccion': kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='referencia'),
                        'sucursalCartera': movimiento['sucursalCartera'],
                        'fechaContable': kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='fechaValor'),
                        'fechaValor': kwargs['task_instance'].xcom_pull(task_ids='recibe_parametros_task', key='fechaValor'),
                        'numeroPago': movimiento['numeroPago'],
                        'codigo': movimiento['codigo'][1:],
                        'montoConcepto': movimiento['montoConcepto'],
                        'ivaConcepto': movimiento['ivaConcepto'],
                        'reportaCobranza': 'null',
                        'sucursalOrigen': movimiento['sucursalOrigen'],
                        'operador': movimiento['operador'],
                        'origenPago': movimiento['origenPago'],
                        'medioPago': movimiento['medioPago'],
                        'tipoOperacion': movimiento['tipoOperacion']
                    })

                responsePostMovimientos = requests.post(url=urlPostMovimientos, json=movimientosPago)
                statusCodePost = responsePostMovimientos.status_code
                responsePostMovimientosJSON = responsePostMovimientos.json()

                print('URL: {}\nRequest: {}\nEstatus: {}\nResponse: {}'.format(urlPostMovimientos, movimientosPago, str(statusCodePost), responsePostMovimientosJSON))

        except:
            hasError = True
            faseSaga = 'movimientos'

    if statusCodeGet != 200 and statusCodePost != 200 and not hasError:
        hasError = True
        faseSaga = 'movimientos'

    if cancelacionHasError and not hasError and fase in ['SALDOS']:

        print('Registra bonificaciones')

        bonificacionComisionJSON = kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', key='bonificacionJSON')
        urlBonificacionComision = 'http://ca-bitacora-bonificaciones-service-v1-stable.new-core.svc.cluster.local/v1/bitacora-bonificacion/'
        bonificacionComision = kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', key='bonificacionComision')

        if bonificacionComision:
            try:
                responseBitacoraBonificacion = requests.post(url=urlBonificacionComision, json=bonificacionComisionJSON)
                statusCode = responseBitacoraBonificacion.status_code
                responseBitacoraBonificacionJSON = responseBitacoraBonificacion.json()

                print('URL: {}\nRequest: {}\nEstatus: {}\nResponse: {}'.format(urlBonificacionComision, bonificacionComisionJSON, str(statusCode), responseBitacoraBonificacionJSON))
            except:
                hasError = True
                faseSaga = 'bonificacion comision'

        print('Registra pagos')

        pagosJSON = kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', key='pagosJSON')
        urlPagos = 'http://ca-pagos-service-v1-stable.new-core.svc.cluster.local/v1/pagos/'
        try:
            responsePagos = requests.post(url=urlPagos, json=pagosJSON)

        except:
            hasError = True
            faseSaga = 'pagos'

    urlSaldosTotales = 'http://core-saldos-service-v1-stable.new-core.svc.cluster.local/v1/saldos/totales/{}'.format(str(idDisposicion))

    if cancelacionHasError and not hasError:
        try:
            responseSaldosTotales = requests.put(url=urlSaldosTotales, json={})
            statusRequest = responseSaldosTotales.status_code
            responseSaldosTotalesJson = responseSaldosTotales.json()

            print('URL: {}\nResponse: {}\nEstatus: {}'.format(urlSaldosTotales, json.dumps(responseSaldosTotalesJson), str(statusRequest)))
        except:
            print('Error llamando saldos-totales')
            statusRequest = 500
            hasError = True
            faseSaga = 'saldos'

    kwargs['task_instance'].xcom_push(key='hasError', value=hasError)
    kwargs['task_instance'].xcom_push(key='faseSaga', value=faseSaga)

    estatusKafka = None

    if cancelacionHasError and not hasError:

        kafkaUrl = 'http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core'

        inputKafka = {
            'status': 'S',
            'mensaje': 'Saga aplicada',
            'idBitacora': kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', key='idBitacoraPago'),
            'operacion': operacion
        }

        try:
            requestKafka = requests.post(url=kafkaUrl, json=inputKafka)
            estatusKafka = requestKafka.status_code

            print('URL: {}, Request: {}, Estatus: {}'.format(kafkaUrl, inputKafka, str(estatusKafka)))
        except:
            print('Error al llamar kafka')
    elif cancelacionHasError and hasError:
        kafkaUrl = 'http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core-error'
        inputKafka = {
            'status': 'F',
            'mensaje': 'Saga no aplicada | Hubo un error al aplicar el servicio {}'.format(faseSaga),
            'idBitacora': kwargs['task_instance'].xcom_pull(task_ids='consulta_bitacora_task', key='idBitacoraPago'),
            'operacion': operacion
        }
        try:
            requestKafka = requests.post(url=kafkaUrl, json=inputKafka)
            estatusKafka = requestKafka.status_code
            print('URL: {}, Request: {}, Estatus: {}'.format(
                kafkaUrl, inputKafka, str(estatusKafka)))
        except:
            print('Error al llamar kafka')
    else:
        print('Saga no aplicada')


with DAG(
    dag_id='NC-17-Saga-reverso',
    default_args=default_args,
    schedule_interval=None
) as dag:

    saga_reverso_task = PythonOperator(
        task_id='saga_reverso_task',
        provide_context=True,
        python_callable=saga_reverso_function
    )

saga_reverso_task
