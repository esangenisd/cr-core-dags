import json
import time
from datetime import datetime, timedelta

import requests
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'FINDEP',
    'start_date': datetime(2021, 8, 5, 18, 0, 0),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}


def reverso_function(ds, **kwargs):

    operacion = 'REVERSO'

    print('Conf: \n' + json.dumps(kwargs['dag_run'].conf, indent=4, sort_keys=True))

    errorRequest = None
    errorResponse = None

    # ========== PARAMETROS ==========

    idBitacoraPago = kwargs['dag_run'].conf['idBitacoraPago']

    idContrato = int(kwargs['dag_run'].conf['contrato'])
    idLineaCredito = int(kwargs['dag_run'].conf['idLineaCredito'])

    referenciaAplicaReverso = str(kwargs['dag_run'].conf['referenciaAplicaReverso'])
    referencia = str(kwargs['dag_run'].conf['referencia'])

    idTransaccionAplicaReverso = referenciaAplicaReverso
    idTransaccion = referencia

    origenPago = str(kwargs['dag_run'].conf['origenPago'])
    medioPago = str(kwargs['dag_run'].conf['medioPago'])

    montoPago = round(float(kwargs['dag_run'].conf['montoPago']), 2)
    montoFondoPago = montoPago

    fechaValor = str(kwargs['dag_run'].conf['fechaValor'])

    isSagaSeguro = bool(kwargs['dag_run'].conf['sagasSeguros'])
    isFondoPago = medioPago == 'FONDO_PAGOS'
    isLiquidacionFondo = False
    isBuscarAnteriores = bool(kwargs['dag_run'].conf['buscarAnteriores'])
    isCancelarUnico = bool(kwargs['dag_run'].conf['cancelarUnico'])

    reversaLedger = True
    if origenPago in ['SEGURO_RENOVACION', 'SEGURO_OTORGAMIENTO']:
        reversaLedger = False

    reversaLedger = True if not '-DevCliCum' in referenciaAplicaReverso else False

    pagosReaplicar = []

    if isBuscarAnteriores:
        urlBitacoraPagos = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/bitacorapagos/search?contrato={idContrato}&status=A'
        print(f'Consultando pagos anteriores: {urlBitacoraPagos}')

        responseBitacoraPagos = requests.get(url=urlBitacoraPagos)
        responseBitacoraPagosJSON = responseBitacoraPagos.json().get('content')
        print(f'Response: {json.dumps(responseBitacoraPagosJSON)}')

        if len(responseBitacoraPagosJSON) != 0:
            pagosReaplicar = cancelarPagosPosteriores(responseBitacoraPagosJSON, idBitacoraPago)

    # ========== LEDGER ==========

    fase = None
    hasError = False

    inputLedger = {
        'transaccionAplicaInverso': idTransaccionAplicaReverso,
        'referenciaAplicaInverso': referenciaAplicaReverso,
        'referencia': referencia,
        'idTransaccion': idTransaccion
    }

    urlLedger = 'http://cr-ledger-service-v1-stable.new-core.svc.cluster.local/v1/ledger/revertirOperacion'

    responseLedger = None
    statusLedger = None
    JSONLedger = None

    if reversaLedger:
        try:
            responseLedger = requests.put(url=urlLedger, json=inputLedger)
            statusLedger = responseLedger.status_code
            JSONLedger = responseLedger.json()

            abonoCCFondo = any(item["tipoCuenta"] == "TC-RALMWN-P9PZ" and item["tipoMovimiento"] == "ABONO" for item in JSONLedger)
            abonoCCCaja = any(item["tipoCuenta"] == "TC-MEPACA-EGOR" and item["tipoMovimiento"] == "ABONO" for item in JSONLedger)

            isLiquidacionFondo = abonoCCFondo and abonoCCCaja

            if isLiquidacionFondo:
                montoFondoPago = next((item["monto"] for item in JSONLedger if item["tipoCuenta"] == "TC-RALMWN-P9PZ"), None)

            print('URL: {}\nRequest: {}\nResponse: {}\nEstatus: {}'.format(urlLedger, inputLedger, json.dumps(JSONLedger), str(statusLedger)))
        except:
            print('Error llamando a ledger')
            statusLedger = 500

        if statusLedger != 201:
            fase = 'LEDGER'
            hasError = True

            errorRequest = inputLedger
            errorResponse = JSONLedger
    else:
        print('No se reversa Ledger debido a que es un pago de PND')

    # ========== MOVIMIENTOS ==========

    inputMovimientos = {
        'idTransaccion': idTransaccionAplicaReverso,
        'fechaValor': fechaValor
    }

    urlCancelacionMovimientos = 'http://cr-movimientos-service-v1-stable.new-core.svc.cluster.local/v1/movimientos/cancelacion/'

    responseMovimientos = None
    statusMovimientos = None
    JSONMovimientos = None

    if not hasError:
        try:
            responseMovimientos = requests.post(url=urlCancelacionMovimientos, json=inputMovimientos)
            statusMovimientos = responseMovimientos.status_code
            JSONMovimientos = responseMovimientos.json()

            print('URL: {}\nRequest: {}\nResponse: {}\nEstatus: {}'.format(urlCancelacionMovimientos, inputMovimientos, json.dumps(JSONMovimientos), str(statusMovimientos)))
        except:
            print('Error llamando a movimientos')
            statusMovimientos = 500

    if statusMovimientos != 200 and not hasError:
        fase = 'MOVIMIENTOS'
        hasError = True

        errorRequest = inputMovimientos
        errorResponse = JSONMovimientos

    # FONDOS
    if isFondoPago or isLiquidacionFondo:
        responseFondo = None
        statusFondo = None

        if not hasError:
            try:
                print('Llamando fondo')
                urlFondo = 'http://broker-api-orchestrator.plataforma.svc.cluster.local/v1/fondo/abono'
                requestFondo = {
                    'credito': str(idLineaCredito),
                    'montoOperacion': montoFondoPago if isLiquidacionFondo else montoPago,
                    'idTransaccion': idTransaccion
                }

                responseFondo = requests.put(url=urlFondo, json=requestFondo)
                statusFondo = responseFondo.status_code
            except:
                statusFondo = 500

        if statusFondo != 200:
            hasError = True
            fase = 'FONDOS'

            errorRequest = requestFondo
            errorResponse = responseFondo.text

    # ========== PAGOS ==========

    urlPagos = 'http://ca-pagos-service-v1-stable.new-core.svc.cluster.local/v1/pagos/referencia/{}'.format(referenciaAplicaReverso)

    responsePagos = None
    statusPagos = None

    if not hasError:
        try:
            responsePagos = requests.delete(url=urlPagos)
            statusPagos = responsePagos.status_code

            print('URL: {}, Estatus: {}'.format(urlPagos, str(statusPagos)))

            if statusPagos == 400:
                print('Response: {}'.format(responsePagos.json()))
        except Exception as e:
            print('Error llamando a pagos: {}'.format(e))
            statusPagos = 500

    if statusPagos != 204 and not hasError:
        hasError = True
        fase = 'PAGOS'

        errorRequest = referenciaAplicaReverso
        errorResponse = None

    # ========== SALDOS ==========

    if not hasError:
        urlSaldos = f'http://core-saldos-service-v1-stable.new-core.svc.cluster.local/v1/saldos/totales/{idLineaCredito}'

        responseSaldos = None

        try:
            responseSaldos = requests.put(url=urlSaldos, json={})

            print(f'URL: {urlSaldos}, Estatus: {responseSaldos.status_code}')

            if responseSaldos.status_code != 200:
                print(f'Response w/err: {responseSaldos.json()}')

        except Exception as e:
            print(f'Error llamando saldos: {e}')
            hasError = True
            fase = 'SALDOS'

            errorRequest = idLineaCredito
            errorResponse = str(e)

    # REAPLICACION
    if len(pagosReaplicar) != 0 and not hasError and isCancelarUnico:
        print(f'Pagos a reaplicar: {json.dumps(pagosReaplicar)}')
        for pagoRequest in pagosReaplicar:
            responseAplicaPago = None
            responseAplicaPagoJSON = None
            print(f'Aplicando pago: {pagoRequest}')

            urlAplicaPago = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/'

            try:
                responseAplicaPago = requests.post(url=urlAplicaPago, json=pagoRequest)
                responseAplicaPagoJSON = responseAplicaPago.json()
                print(f'Response: {responseAplicaPagoJSON}')

                if responseAplicaPago.status_code != 200:
                    break

                esperarAplicacionPago(responseAplicaPagoJSON.get('referencia'))
            except Exception as e:
                print('Error:', str(e))
                hasError = True
                fase = 'REAPLICACION'

    # ========== KAFKA ==========

    if not hasError:
        print('Llamando Kafka: Exito')

        inputKafka = {
            'status': 'S' if isSagaSeguro else 'R',
            'mensaje': 'EXITO',
            'idBitacora': idBitacoraPago,
            'operacion': operacion
        }

        requestKafka = requests.post(url='http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core', json=inputKafka)
        statusRequestKafka = requestKafka.status_code

        print('Request: {}\nEstatus: {}'.format(inputKafka, str(statusRequestKafka)))
    else:

        print('Publicando error')

        inputKafka = {
            'request:': errorRequest,
            'response': errorResponse,
            'status': 'E',
            'mensaje': 'Ocurrio un error al invocar el servicio {}'.format(fase),
            'idBitacora': idBitacoraPago,
            'operacion': operacion
        }

        requestKafka = requests.post(url='http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core-error', json=inputKafka)
        statusCodeKafka = requestKafka.status_code

        print('Request: {}\nEstatus: {}'.format(inputKafka, str(statusCodeKafka)))

        kwargs['ti'].xcom_push(key='idBitacoraPago', value=idBitacoraPago)
        kwargs['ti'].xcom_push(key='referencia', value=referencia)
        kwargs['ti'].xcom_push(key='referenciaAplicaReverso', value=referenciaAplicaReverso)
        kwargs['ti'].xcom_push(key='fechaValor', value=fechaValor)
        kwargs['ti'].xcom_push(key='fase', value=fase)

    if hasError:
        return 'saga_call_task'
    else:
        raise AirflowSkipException('El reverso no tuvo error')


def esperarAplicacionPago(idBitacoraPago):
    statusPago = 'P'
    isAplicandoPago = True

    try:
        print(f'Esperando aplicación del pago:{idBitacoraPago}')
        urlBitacoraPago = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/bitacorapagos/{idBitacoraPago}'

        while isAplicandoPago:
            responseBitacora = requests.get(url=urlBitacoraPago)
            print(f'{responseBitacora}')
            bitacoraJSON = responseBitacora.json()
            print(f'Estatus aplicacion: {bitacoraJSON.get("status")}')

            statusPago = bitacoraJSON.get('status')
            if statusPago in ('A', 'E', 'S', 'F'):
                isAplicandoPago = False

            time.sleep(5)

    except Exception as e:
        print(f'Ha ocurrido un error en la consulta del reverso: {idBitacoraPago} | {e.with_traceback()}')

    print(f'Se ha resuelto espera de pago con status: {statusPago}')
    return statusPago


def cancelarPagosPosteriores(pagos, idBitacoraActual):
    statusCancelacion = []
    inputReaplicar = []

    bitacorasPosteriores = [objeto for objeto in pagos if objeto["idBitacoraPago"] > idBitacoraActual]
    pagos = sorted(bitacorasPosteriores, key=lambda x: x.get('idBitacoraPago'), reverse=False)

    for pago in pagos[::-1]:
        try:
            dtFechaValorReaplicar = datetime.strptime(pago.get('fechaValor'), "%Y-%m-%d %H:%M:%S")
            
            empresa = None
            if pago.get('idTenant') == 100000000:
                empresa = 'FISA'
            elif pago.get('idTenant') == 100000004:
                empresa = 'AEF'

            inputReaplicar.append({
                'contrato': pago.get('contrato'),
                'empresa': empresa,
                'fechaValor': dtFechaValorReaplicar.strftime("%Y-%m-%d"),
                'medioPago': pago.get('medioPago'),
                'origenPago': pago.get('origenPago'),
                'monto': pago.get('monto'),
                'referenciaPago': pago.get('referenciaPago') + '-R',
                'caja': pago.get('caja'),
                'operador': pago.get('operador'),
                'sucursalOrigen': pago.get('sucursalOrigen'),
                'auth': pago.get('auth'),
                'folio': pago.get('folio'),
                'tipoAbono': 'MONTO',
                'abonoCapital': False,
                'buscaAnteriores': False
            })

            urlReversaPago = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/{pago.get("idBitacoraPago")}'

            ref = datetime.now().strftime("%Y%m%d%H%M%S")

            inputReversarPago = {
                'medioDeCancelacion': pago.get('medioPago'),
                'mensajeCancelacion': 'Cancelado por orden de aplicacion',
                'referenciaCancelacion': ref,
                'buscarAnteriores': False,
                'cancelarUnico': False
            }

            response = requests.post(url=urlReversaPago, json=inputReversarPago)

            print(f'Reversando pago | URL:{pago.get("idBitacoraPago")}, Request: {inputReversarPago}')
            print(f'Response: {response}')

            if response.status_code != 200:
                break

            statusReverso = esperarReversoPago(pago.get('idBitacoraPago'))

            if statusReverso == 'R':
                statusCancelacion.append(True)
            elif statusReverso == 'S' or statusReverso == 'F' or statusReverso == 'E':
                statusCancelacion.append(False)
                break

        except Exception as e:
            print(f'Ha ocurrido un error al cancelar pagos: {e.with_traceback()}')

    print(f'Estatus cancelaciones: {statusCancelacion}')

    return statusCancelacion


def esperarReversoPago(idBitacoraPago):
    statusPago = 'A'
    isAplicandoReverso = True

    try:
        print(f'Esperando aplicación de reverso:{idBitacoraPago}')
        urlBitacoraPago = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/bitacorapagos/{idBitacoraPago}'

        while isAplicandoReverso:
            responseBitacora = requests.get(url=urlBitacoraPago)
            bitacoraJSON = responseBitacora.json()
            print(f'Estatus reverso: {bitacoraJSON.get("status")}')

            statusPago = bitacoraJSON.get('status')
            if statusPago in ('R', 'S', 'F'):
                isAplicandoReverso = False

            time.sleep(5)

    except Exception as e:
        print(f'Ha ocurrido un error en la consulta del reverso: {idBitacoraPago} | {e.with_traceback()}')


with DAG(
        dag_id='NC-14-Reverso-pago',
        default_args=default_args,
        schedule_interval=None
) as dag:

    reverso_task: BranchPythonOperator = BranchPythonOperator(
        task_id='reverso_task',
        provide_context=True,
        python_callable=reverso_function
    )

    trigger_task: TriggerDagRunOperator = TriggerDagRunOperator(
        task_id='saga_call_task',
        trigger_dag_id='NC-17-Saga-reverso',
        conf={
            'idBitacoraPago': "{{ti.xcom_pull(task_ids='reverso_task', key='idBitacoraPago')}}",
            'referenciaAplicaReverso': "{{ti.xcom_pull(task_ids='reverso_task', key='referenciaAplicaReverso')}}",
            'referencia': "{{ti.xcom_pull(task_ids='reverso_task', key='referencia')}}",
            'fechaValor': "{{ti.xcom_pull(task_ids='reverso_task', key='fechaValor')}}",
            'fase': "{{ti.xcom_pull(task_ids='reverso_task', key='fase')}}"
        }
    )


reverso_task >> [trigger_task]
