import json
import time
import requests

from enum import Enum
from datetime import datetime, timedelta
from math import fsum

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'FINDEP',
    'start_date': datetime(2021, 8, 5, 18, 0, 0),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}


def pago_function(ds, **kwargs):

    operacion = 'PAGO'

    print(f'Conf : \n{json.dumps(kwargs["dag_run"].conf, indent=4, sort_keys=True)}')

    errorRequest = None
    errorResponse = None

    # =============== PARAMETROS ===============

    # Bitacoras
    idBitacoraPagoDevSeguro = kwargs['dag_run'].conf['idBitacoraPagoDevSeguro']
    idBitacoraSeguros = int(kwargs['dag_run'].conf['idBitacoraSeguros'])
    idBitacora = int(kwargs['dag_run'].conf['idBitacora'])

    # Info. empresa
    claveEmpresa = str(kwargs['dag_run'].conf['claveEmpresa'])
    operador = str(kwargs['dag_run'].conf['operador'])

    # Info. contrato
    idDisposicion = int(kwargs['dag_run'].conf['idDisposicion'])
    lineaCredito = int(kwargs['dag_run'].conf['lineaCredito'])
    contrato = int(kwargs['dag_run'].conf['contrato'])

    estatusCartera = str(kwargs['dag_run'].conf['estatusCartera'])
    etapaCarteraDiario = str(kwargs['dag_run'].conf['etapaCarteraDiario'])
    sucursalCartera = int(kwargs['dag_run'].conf['sucursalCartera'])
    sucursalOrigen = int(kwargs['dag_run'].conf['sucursalOrigen'])

    centroCostos = sucursalCartera

    propietario = str(kwargs['dag_run'].conf['propietario'])

    diasVencidos = int(kwargs['dag_run'].conf['diasVencidos'])

    # Info. pago
    fechaContable = str(kwargs['dag_run'].conf['fechaContable'])
    fechaValor = str(kwargs['dag_run'].conf['fechaValor'])

    dtFechaValor = datetime.strptime(fechaValor, '%Y-%m-%d %H:%M:%S')
    dtFechaContable = datetime.strptime(fechaContable, '%Y-%m-%d %H:%M:%S')

    origenPago = str(kwargs['dag_run'].conf['origenPago'])
    medioPago = str(kwargs['dag_run'].conf['medioPago'])

    tipoOperacion = str(kwargs['dag_run'].conf['tipoOperacion'])
    tipoQuebranto = str(kwargs['dag_run'].conf['tipoQuebranto'])
    tipoPagoSeguro = str(kwargs['dag_run'].conf['tipoPagoSeguro'])

    montoFondoPagoArg = kwargs['dag_run'].conf['montoFondoPago']
    montoFondoPago = 0 if montoFondoPagoArg == None else float(montoFondoPagoArg)

    montoPago = round(float(kwargs['dag_run'].conf['montoPago']), 2)
    montoParaLiquidar = float(kwargs['dag_run'].conf['montoParaLiquidar'])

    idTransaccion = str(kwargs['dag_run'].conf['idTransaccion'])
    referencia = str(kwargs['dag_run'].conf['referencia'])

    desglosePrima = kwargs['dag_run'].conf['desglosePrima']
    desglosePrimaForLedger = []

    # Banderas
    isPrimaNoDevengada = bool(kwargs['dag_run'].conf['primaNoDevengada'])
    isBonificacionComision = bool(kwargs['dag_run'].conf['bonificacionComision'])
    isAbonoCapital = bool(kwargs['dag_run'].conf['abonoCapital'])
    isPagoClienteCumplido = bool(kwargs['dag_run'].conf['pagoClienteCumplido'])
    isSeguroRenovacioUOtorgamiento = medioPago in ['SEGURO_OTORGAMIENTO', 'SEGURO_RENOVACION']
    isSaldoFavorFondo = bool(kwargs['dag_run'].conf['saldoFavorFondo'])
    isBuscaAnteriores = bool(kwargs['dag_run'].conf['buscarAnteriores'])

    isPagoFondo = medioPago == 'FONDO_PAGOS'
    isMontoFondoPagoDifZero = montoFondoPago != 0

    tipoAbono = str(kwargs['dag_run'].conf['tipoAbono'])
    liquidacionSeguro = bool(kwargs['dag_run'].conf['liquidacionSeguro'])

    hasBonificacion = False
    hasError = False
    fase = None

    tipoOperacion = 'PAGO_VENCIDO' if diasVencidos > 0 else tipoOperacion
    operador = 0 if operador == 'None' else operador
    isCarteraVencida = True if etapaCarteraDiario in ['ETAPA 4', 'ETAPA 5'] else False

    isQuebranto = True if tipoQuebranto != 'NO_QUEBRANTO' else False
    isPagoSeguro = True if tipoPagoSeguro != 'NO_PAGO_SEGURO' else False

    tipoQuebrantoPago = obtenerCodigoQuebratoPago(tipoQuebranto)

    diferenciaPago = montoParaLiquidar - montoPago
    isBonificacionSaldoMinimo = True if 0.0 < diferenciaPago < 1.0 else False
    print(f'Pago es una liquidacion por saldo minimo: {isBonificacionSaldoMinimo}')

    aplicaLedger = False if origenPago == 'SEGURO_RENOVACION' or origenPago == 'SEGURO_OTORGAMIENTO' else True
    aplicaLedger = False if isPagoClienteCumplido else True

    if desglosePrima != None:
        for prima in desglosePrima:
            prima['montoPrimaNoDev'] = round(prima['montoPrimaNoDev'], 2)
            primaLedger = {
                'tipoOperacion': prima['tipoOperacion'],
                'montoPrimaNoDev': prima['montoPrimaNoDev']
            }

            desglosePrimaForLedger.append(primaLedger)

        montoPago = round(montoPago, 2)
        print('Proceso PND: Formateado a dos digitos desglose prima y monto pago')

    # =============== CONSULTA BONIFICACIONES ===============

    montoParaPagarBonificacion = None

    responseConsultaBonificaciones = None
    responseConcultaBonificacionesJSON = None

    containsBonificacionActiva = False
    appliesBonificacion = False
    isInFechaAplicable = False

    dtFechaAplicacionBonificacion = None
    dtFechaLimiteBonificacion = None

    if not isPrimaNoDevengada:
        try:
            urlConsultaBonificacion = f'http://ca-bitacora-bonificaciones-service-v1-stable.new-core.svc.cluster.local/v1/bitacora-bonificacion/search?idLineaCredito={lineaCredito}&status=NUEVO'

            print(f'Consultando bonificaciones: {urlConsultaBonificacion}')

            responseConsultaBonificaciones = requests.get(url=urlConsultaBonificacion)

            print(f'Request: idLineaCredito={lineaCredito}&status=NUEVO | SCode: {responseConsultaBonificaciones.status_code} | Response: {responseConsultaBonificaciones}')

            if responseConsultaBonificaciones.status_code == 200:
                responseConcultaBonificacionesJSON = responseConsultaBonificaciones.json().get('content')

                print(f'URL: {urlConsultaBonificacion}\nEstatus: {responseConsultaBonificaciones.status_code}\nResponse: {json.dumps(responseConcultaBonificacionesJSON)}')

                if len(responseConcultaBonificacionesJSON) == 0:
                    print('No se contiene una bonificación activa')
                else:
                    responseConcultaBonificacionesJSON = responseConcultaBonificacionesJSON[0]
                    containsBonificacionActiva = True

                    dtFechaAplicacionBonificacion = datetime.strptime(responseConcultaBonificacionesJSON.get('fechaCreacion'), '%Y-%m-%dT%H:%M:%S.%f%z')
                    dtFechaLimiteBonificacion = datetime.strptime(responseConcultaBonificacionesJSON.get('fechaLimite'), '%Y-%m-%d').replace(hour=23, minute=59, second=59)

                    print(f'Fecha aplicación bonificación: {dtFechaAplicacionBonificacion.date()} {dtFechaAplicacionBonificacion.time()}')
                    print(f'Fecha contable pago: {dtFechaContable.date()} {dtFechaContable.time()}')
                    print(f'Fecha limite bonificación: {dtFechaLimiteBonificacion.date()} {dtFechaLimiteBonificacion.time()}')

                    isInFechaAplicable = True if dtFechaAplicacionBonificacion.date() <= dtFechaContable.date() <= dtFechaLimiteBonificacion.date() else False
                    appliesBonificacion = True if round(responseConcultaBonificacionesJSON.get('montoPago'), 2) == montoPago else False

                    montoParaPagarBonificacion = round(responseConcultaBonificacionesJSON.get('montoPago'), 2)
        except Exception as e:
            print(f'Ha ocurrido un error en la consuta de bonificaciones: {e}')

    # =============== CONSULTA PAGOS ANTERIORES ===============

    responseBitacoraPagos = None
    responseBitacoraPagosJSON = None

    isEliminaPagosAnteriores = False
    isAplicaPagoParaBonificar = False

    if not appliesBonificacion and containsBonificacionActiva and isInFechaAplicable:
        try:
            urlBitacoraPagos = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/bitacorapagos/search?contrato={contrato}&idDisposicion={idDisposicion}&status=A&pageSize=1000'
            print(f'Consultando pagos anteriores (bonificacion parcialidades): {urlBitacoraPagos}')

            responseBitacoraPagos = requests.get(url=urlBitacoraPagos)
            responseBitacoraPagosJSON = responseBitacoraPagos.json().get('content')
            print(f'Response: {responseBitacoraPagosJSON}')

            pagosEnFecha = obtenerPagosEnFecha(responseBitacoraPagosJSON, dtFechaAplicacionBonificacion, dtFechaLimiteBonificacion)

            if len(pagosEnFecha) != 0:
                totalPagosEnFecha = obtenerTotalPagos(pagosEnFecha)
                totalAcumulado = round(totalPagosEnFecha + montoPago, 2)

                print(f'Pagos en fecha: {len(pagosEnFecha)} sumando: {totalPagosEnFecha} además de {montoPago} = {totalAcumulado} donde son {montoParaPagarBonificacion} para bonificar')

                isEliminaPagosAnteriores = True if totalAcumulado >= montoParaPagarBonificacion else False

        except Exception as e:
            print(f'Ha ocurrido un error al buscar los pagos: {e}')

    if isEliminaPagosAnteriores:
        reversados = cancelarPagosAnteriores(pagosEnFecha, Motivo.ACTIVACION_BONIFICACION)

        isAplicaPagoParaBonificar = True if all(reversados) else False

    # =============== CONSULTA PAGOS FECHA VALOR ===============
    resBitPagoAnteriores = None
    resBitPagoAnterioresJSON = None

    pagosReaplicar = []
    if isBuscaAnteriores:
        urlBitacoraPagosAnteriores = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/bitacorapagos/search/fechaValor?contrato={contrato}&fechaValor={fechaValor}'
        print(f'Buscando pagos anteriores (pagos fecha valor): {urlBitacoraPagosAnteriores}')

        resBitPagoAnteriores = requests.get(url=urlBitacoraPagosAnteriores)
        resBitPagoAnterioresJSON = resBitPagoAnteriores.json()
        print(f'Response: {json.dumps(resBitPagoAnterioresJSON)}')

        listaPagosPosteriores = pagosPosteriores(resBitPagoAnterioresJSON, fechaValor)

        if len(listaPagosPosteriores) != 0:
            cancelarPagosAnteriores(listaPagosPosteriores, Motivo.INTERMEDIO)

            for pago in listaPagosPosteriores:
                empresa = None
                if pago.get('idTenant') == 100000000:
                    empresa = 'FISA'
                elif pago.get('idTenant') == 100000004:
                    empresa = 'AEF'

                dtFechaValorReaplicar = datetime.strptime(pago.get('fechaValor'), "%Y-%m-%d %H:%M:%S")

                pagosReaplicar.append({
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

    # =============== BONIFICACION FONDO ===============
    responseBonificacionFondo = None
    statusBonificacionFondo = None
    responseBonificacionFondoJSON = None
    inputBonificacionFondo = None

    if isMontoFondoPagoDifZero:
        try:
            print('Llamando bonificacion fondo')

            inputBonificacionFondo = {
                'idLineaCredito': lineaCredito,
                'fechaValor': str(dtFechaValor.date()),
                'sentido': 'FONDO',
                'bonificacion': montoFondoPago
            }

            urlBonificacionFondo = 'http://ca-bitacora-bonificaciones-service-v1-stable.new-core.svc.cluster.local/v1/bitacora-bonificacion/'

            responseBonificacionFondo = requests.post(url=urlBonificacionFondo, json=inputBonificacionFondo)
            statusBonificacionFondo = responseBonificacionFondo.status_code
            responseBonificacionFondoJSON = responseBonificacionFondo.json()

            print(f'URL: {urlBonificacionFondo}\nRequest: {inputBonificacionFondo},\nEstatus: {str(statusBonificacionFondo)},\nResponse: {responseBonificacionFondoJSON}')

        except Exception as e:
            print(f'Excepcion: {e}')
            statusBonificacionFondo = 500
    else:
        statusBonificacionFondo = 201

    if statusBonificacionFondo != 201:
        hasError = True
        fase = 'BONIFICACION_FONDO'

        errorRequest = inputBonificacionFondo
        errorResponse = responseBonificacionFondoJSON

    # =============== BONIFICACION COMISION ===============

    responseBitacoraBonificacion = None
    statusRequestBitacoraBonificacion = None
    responseBitacoraBonificacionJSON = None
    inputBitacoraBonificaciones = None

    if isBonificacionComision and not hasError:
        try:
            print('Llamando comision bonificacion')
            fechaLimite = dtFechaValor.today() + timedelta(days=1)

            inputBitacoraBonificaciones = {
                'idLineaCredito': lineaCredito,
                'fechaValor': str(dtFechaValor.date()),
                'fechaLimite': datetime.strftime(fechaLimite, '%Y-%m-%d'),
                'empresa': 'CORE',
                'sentido': 'CONTRATO',
                'medioPago': medioPago,
                'montoPago': montoPago,
                'objetivo': 'COMISION'
            }

            urlBonificacionComision = 'http://ca-bitacora-bonificaciones-service-v1-stable.new-core.svc.cluster.local/v1/bitacora-bonificacion/'

            responseBitacoraBonificacion = requests.post(url=urlBonificacionComision, json=inputBitacoraBonificaciones)
            statusRequestBitacoraBonificacion = responseBitacoraBonificacion.status_code
            responseBitacoraBonificacionJSON = responseBitacoraBonificacion.json()

            print(f'URL: {urlBonificacionComision}\nRequest: {inputBitacoraBonificaciones},\nEstatus: {str(statusRequestBitacoraBonificacion)},\nResponse: {responseBitacoraBonificacionJSON}')
        except:
            print('Error llamando bitacora-comision')
            statusRequestBitacoraBonificacion = 500
    else:
        statusRequestBitacoraBonificacion = 201

    if statusRequestBitacoraBonificacion != 201:
        hasError = True
        fase = 'BONIFICACION_COMISION'

        errorRequest = inputBitacoraBonificaciones
        errorResponse = responseBitacoraBonificacionJSON

    # =============== PAGOS ===============

    responsePagos = None
    pagosJson = None
    statusCodePagos = None

    if not hasError and not isPagoClienteCumplido and not isSeguroRenovacioUOtorgamiento and not isSaldoFavorFondo and not isPrimaNoDevengada:
        try:
            print('Llamando pagos')

            inputPagos = {
                'disposicion': idDisposicion,
                'idLineaCredito': lineaCredito,
                'referencia': referencia,
                'tipoAbono': tipoAbono if isAbonoCapital else None,
                'carteraVencida': isCarteraVencida,
                'primaNoDevengada': isPrimaNoDevengada,
                'pagoClienteCumplido': isPagoClienteCumplido
            }

            if isQuebranto:
                inputPagos['quebranto'] = tipoQuebrantoPago
            elif isPagoSeguro:
                inputPagos['quebranto'] = 'SEG'

            if isAplicaPagoParaBonificar:
                inputPagos['fechaValor'] = str(dtFechaAplicacionBonificacion.date())
                inputPagos['montoPago'] = totalAcumulado
            else:
                inputPagos['fechaValor'] = str(dtFechaValor.date())
                inputPagos['montoPago'] = montoParaLiquidar if isBonificacionSaldoMinimo else montoPago

            urlPagos = 'http://ca-pagos-service-v1-stable.new-core.svc.cluster.local/v1/pagos/'

            responsePagos = requests.post(url=urlPagos, json=inputPagos)
            statusCodePagos = responsePagos.status_code
            pagosJson = responsePagos.json()

            print(f'URL {urlPagos}\nRequest: {inputPagos}\nEstatus: {statusCodePagos}\nResponse: {json.dumps(pagosJson)}')
        except:
            print('Error llamando pagos')
            statusCodePagos = 500
    else:
        print('Omitida llamada a pagos')
        print(f'Cliente cumplido: {not isPagoClienteCumplido} | Seguro Renovacion/Otorgamiento: {not isSeguroRenovacioUOtorgamiento} | Saldo favor fondo: {isSaldoFavorFondo} | Prima no devengada: {not isPrimaNoDevengada}')
        statusCodePagos = 201

    if statusCodePagos != 201 and not hasError:
        print('El pago tuvo error', statusCodePagos)
        hasError = True
        fase = 'PAGOS'

        errorRequest = inputPagos
        errorResponse = pagosJson

    # =============== FONDOS ===============
    urlFondo = 'http://broker-api-orchestrator.plataforma.svc.cluster.local/v1/fondo/cargo'

    responseFondo = None
    statusFondo = None
    responseFondoJSON = None

    requestFondo = {
        'credito': str(lineaCredito),
        'montoOperacion': montoFondoPago if isMontoFondoPagoDifZero else montoPago,
        'idTransaccion': idTransaccion
    }

    if not hasError and (isPagoFondo or isMontoFondoPagoDifZero):
        try:
            print('Llamando fondos')

            responseFondo = requests.put(url=urlFondo, json=requestFondo)
            statusFondo = responseFondo.status_code
            print(f'Response fondo: {responseFondo.text}')

            print(f'URL: {urlFondo}\nRequest: {requestFondo}\nEstatus:{statusFondo}\nResponse: {json.dumps(responseFondoJSON)}')
        except Exception as e:
            print(f'Error llamando fondos {e}')
            statusRequest = 500

        if statusFondo != 200 and not hasError:
            hasError = True
            fase = 'FONDO'

            errorRequest = requestFondo
            errorResponse = responseFondoJSON

    # =============== OBJ. MOVIMIENTOS-LEDGER ===============

    inputMovimientos = []
    inputLedger = []

    hasSaldoFavor = False
    montoSaldoFavor = 0
    tipoOperacionDesgloseSaldoFavor = None
    capitalInsolutoRestanteSaldoFavor = 0

    # MONTOS LIQUIDACION
    montosCapitalLiquidacion = []

    montosGCLiquidacion = []
    montosIvaGCLiquidacion = []

    montosInteresesLiquidacion = []
    montosIvaInteresesLiquidacion = []

    montosCapitalQuebranto = []

    montosGCQuebranto = []
    montosIvaGCQuebranto = []

    montosInteresQuebranto = []
    montosIvaInteresQuebranto = []

    # MONTOS PAGO
    operacionesledger = []

    cargoCapitalLedger = 0.0

    cargoCapitalLedgerFondo = 0.0

    cargoInteresLedger = 0.0
    cargoIvaInteresLedger = 0.0

    cargoInteresFondo = 0.0
    cargoIvaInteresesFondo = 0.0

    cargoInteresNoContableLedger = 0.0
    cargoIvaInteresNoContableLedger = 0.0

    cargoCobranzaLedger = 0.0
    cargoIvaCobranzaLedger = 0.0

    cargoCobranzaFondo = 0.0
    cargoIvaCobranzaFondo = 0.0

    cargoSaldoAFavor = 0.0

    # MONTOS BONIFICACION
    operacionesBonificacionLedger = []

    cargoCapitalBonificacionLedger = 0.0

    cargoInteresBonificacionLedger = 0.0
    cargoIvaInteresBonificacionLedger = 0.0

    cargoInteresNoContableBonificacionLedger = 0.0
    cargoIvaInteresNoContableBonificacionLedger = 0.0

    cargoCobranzaBonificacionLedger = 0.0
    cargoIvaCobranzaBonificacionLedger = 0.0

    cargoInteresSimbolicoLedger = 0.0
    cargoIvaInteresSimbolicoLedger = 0.0

    # BANDERAS
    isLiquidado = False

    posicionSeguro = 0
    ivaDescontado = 0
    montoDescontado = 0

    isDescontando = True
    isDescontandoIVA = True

    saldoFinalExtension = 0
    isBonificacionExtension = False

    movimientoRequestBase = {
        'idDisposicion': idDisposicion,
        'idLineaCredito': lineaCredito,
        'contrato': contrato,
        'idTenant': claveEmpresa,
        'idTransaccion': idTransaccion,
        'sucursalCartera': sucursalCartera,
        'sucursalOrigen': sucursalOrigen,
        'fechaContable': str(dtFechaContable.date()),
        'fechaValor': str(dtFechaValor.date()),
        'reportaCobranza': 'null',
        'operador': operador,
        'origenPago': origenPago,
        'medioPago': medioPago
    }

    if not hasError and not isPagoClienteCumplido and not isSaldoFavorFondo and not isPrimaNoDevengada:
        print('Iterando sobre respuesta pagos.')

        isLiquidado = bool(pagosJson.get('liquidado'))

        codigoMov = ''
        ivaConcepto = 0
        montoConcepto = 0

        noPagosCubiertos = len(pagosJson['desglose'])
        print(f'Pagos cubiertos: {noPagosCubiertos}')

        contieneBonificacion = any(movimiento['tipo'] == 'BONIFICACION' for movimiento in pagosJson['desglose'])
        isBonificacionExtension = any(movimiento['tipoOperacion'] == 'EXTENSION' for movimiento in pagosJson['desglose'])

        print(f'Pago contiene bonificaciones: {contieneBonificacion}')
        print(f'Pago contiene extensión: {isBonificacionExtension}')

        for i, movPago in enumerate(pagosJson['desglose']):

            isBonificacionFondo = movPago.get('tipo') == 'BONIFICACION' and movPago.get('tipoOperacion') == 'FONDO'

            isDynamicBonification = False

            isUltimoPago = False
            if (i+1) == noPagosCubiertos:
                isUltimoPago = True

            capital = float(movPago.get('capital'))

            interesProyectado = float(movPago.get('interes'))
            ivaInteresProyectado = float(movPago.get('ivaInteres'))

            interesContable = float(movPago.get('interesContable'))
            ivaInteresContable = float(movPago.get('ivaInteresContable'))

            gastoCobranza = float(movPago.get('gastosCobranza'))
            ivaGastoCobranza = float(movPago.get('ivaGastosCobranza'))

            interesNoContable = round(interesProyectado - interesContable, 2)
            ivaInteresNoContable = round(ivaInteresProyectado - ivaInteresContable, 2)

            interesSimbolico = round(float(movPago.get('interesSimbolico')), 2)
            ivaInteresSimbolico = round(float(movPago.get('ivaInteresSimbolico')), 2)

            containsInteresSimbolico = True if interesSimbolico != 0.0 or ivaInteresSimbolico != 0.0 else False

            saldoFinal = float(movPago.get('saldoFinal'))

            isExedente = movPago.get('exedente')

            containsInteresNoContable = True if interesNoContable != 0.0 or ivaInteresNoContable != 0.0 else False

            print(f'Contiene intereses no contables: {containsInteresNoContable}')
            msgConceptos = {
                'capital': capital if not isExedente else 0.0,  # CAPITAL
                'interes_proyectado': interesProyectado,  # INTERES
                'iva_interes_proyectado': ivaInteresProyectado,  # IVA INTERES
                'interes_contable': interesContable,  # INTERES CONTABLE (EPRC)
                'iva_interes_contable': ivaInteresContable,  # IVA INTERES CONTABLE (EPRC)
                'interes_no_contable': interesNoContable,  # INTERES NO CONTABLE
                'iva_interes_no_contable': ivaInteresNoContable,  # IVA INTERES NO CONTABLE
                'interes_simbolico': interesSimbolico,  # INTERES SIMBOLICO
                'iva_interes_simbolico': ivaInteresSimbolico,  # IVA INTERES SIMBOLICO
                'gasto_cobranza': gastoCobranza,  # GASTO COBRANZA
                'iva_gasto_cobranza': ivaGastoCobranza,  # IVA GASTO COBRANZA
                'saldo_favor': capital if isExedente else 0.0,  # SALDO FAVOR
                'saldo_insoluto': saldoFinal  # SALDO INSOLUTO
            }

            noPago = movPago.get('noPago')

            msgDesglose = f'{movPago.get("tipo")} #{noPago} {movPago.get("tipoDePago")} DE TIPO {movPago.get("tipoOperacion")} | '
            msgDesglose += str(json.dumps(msgConceptos, indent=4))
            print(msgDesglose)

            tipoOperacionDesglose = 'BONIFICACION' if 'BONIFICACION' in movPago.get('tipo') else tipoOperacion

            capitalInsolutoRestante = movPago.get('saldoFinal')
            if i == len(pagosJson['desglose']) - 1:
                saldoFinalExtension = capitalInsolutoRestante

            if movPago.get('tipo') == 'PAGO':
                if not isExedente:
                    cargoCapitalLedger = cargoCapitalLedger + capital
                else:
                    cargoSaldoAFavor = abs(cargoSaldoAFavor + capital)

                cargoInteresLedger = cargoInteresLedger + interesProyectado
                cargoIvaInteresLedger = cargoIvaInteresLedger + ivaInteresProyectado

                cargoCobranzaLedger = cargoCobranzaLedger + gastoCobranza
                cargoIvaCobranzaLedger = cargoIvaCobranzaLedger + ivaGastoCobranza

            if movPago.get('tipo') == 'BONIFICACION':
                if not isExedente:
                    cargoCapitalBonificacionLedger = round(cargoCapitalBonificacionLedger + capital, 2)
                    cargoCapitalLedgerFondo = cargoCapitalLedgerFondo + capital if isBonificacionFondo else 0

                if containsInteresNoContable:
                    cargoInteresNoContableBonificacionLedger = round(cargoInteresNoContableBonificacionLedger + interesNoContable, 2)
                    cargoIvaInteresNoContableBonificacionLedger = round(cargoIvaInteresNoContableBonificacionLedger + ivaInteresNoContable, 2)

                    cargoInteresFondo = cargoInteresFondo + interesNoContable if isBonificacionFondo else 0
                    cargoIvaInteresesFondo = cargoIvaInteresesFondo + ivaInteresNoContable if isBonificacionFondo else 0
                else:
                    cargoInteresBonificacionLedger = round(cargoInteresBonificacionLedger + interesContable, 2)
                    cargoIvaInteresBonificacionLedger = round(cargoIvaInteresBonificacionLedger + ivaInteresContable, 2)

                    cargoInteresFondo = cargoInteresFondo + interesContable if isBonificacionFondo else 0
                    cargoIvaInteresesFondo = cargoIvaInteresesFondo + ivaInteresContable if isBonificacionFondo else 0

                cargoInteresSimbolicoLedger = round(cargoInteresSimbolicoLedger + interesSimbolico, 2)
                cargoIvaInteresSimbolicoLedger = round(cargoIvaInteresSimbolicoLedger + ivaInteresSimbolico, 2)

                cargoCobranzaBonificacionLedger = round(cargoCobranzaBonificacionLedger + gastoCobranza, 2)
                cargoIvaCobranzaBonificacionLedger = round(cargoIvaCobranzaBonificacionLedger + ivaGastoCobranza, 2)

                cargoCobranzaFondo = cargoCobranzaFondo + gastoCobranza if isBonificacionFondo else 0
                cargoIvaCobranzaFondo = cargoIvaCobranzaFondo + ivaGastoCobranza if isBonificacionFondo else 0

            if (gastoCobranza != 0.0 or ivaGastoCobranza != 0.0):
                montoConcepto = gastoCobranza
                ivaConcepto = ivaGastoCobranza

                codigoMov = ''
                subcodigo = ''

                if 'BONIFICACION' in movPago.get('tipo') and not movPago.get('tipoOperacion') == 'FONDO':
                    if (movPago.get('tipoOperacion') == 'COMISION'):
                        codigoMov = 'BGCC'
                    if (movPago.get('tipoOperacion') == 'CURA'):
                        codigoMov = 'BCVG'
                    if (movPago.get('tipoOperacion') == 'PROMOCION'):
                        codigoMov = 'BCGC'
                    if (movPago.get('tipoOperacion')[1:-1] == 'X'):
                        codigoMov = 'BNHG'
                        isDynamicBonification = True
                        subcodigo = movPago.get('tipoOperacion')

                    if (movPago.get('tipoOperacion') == 'LIQUIDAR'):
                        codigoMov = 'BLVG'
                    if (movPago.get('tipoOperacion') == 'QUITA'):
                        codigoMov = 'BQTG'
                    if (movPago.get('tipoOperacion') == 'EXTENSION'):
                        codigoMov = 'BECO'
                    if (movPago.get('tipoOperacion') == 'ACLARACION'):
                        codigoMov = 'BAGC'
                    if (movPago.get('tipoOperacion') == 'REMANENTE'):
                        codigoMov = 'BRGC'

                else:
                    codigoMov = 'PGGC' if not isQuebranto else obtenerCodigoQuebranto(tipoQuebranto, 'GASTO_COBRANZA')

                if isLiquidado and movPago.get('tipoDePago') != 'ATRASADO' and movPago.get('tipo') != 'BONIFICACION' and not isQuebranto:
                    print(f'Agregando montos de liquidacion GASTO_COBRANZA: {montoConcepto}, {ivaConcepto}')

                    montosGCLiquidacion.append(montoConcepto)
                    montosIvaGCLiquidacion.append(ivaConcepto)
                elif isQuebranto:
                    montosGCQuebranto.append(montoConcepto)
                    montosIvaGCQuebranto.append(ivaConcepto)
                else:
                    if isPrimaNoDevengada:
                        isDescontando = True
                        isDescontandoIVA = True

                        while isDescontando:
                            if ivaConcepto == 0:
                                isDescontandoIVA = False

                            if isDescontandoIVA:
                                ivaDescontado = round(ivaDescontado + 0.01, 2)
                                ivaConcepto = round(ivaConcepto - 0.01, 2)
                            else:
                                montoDescontado = round(montoDescontado + 0.01, 2)
                                montoConcepto = round(montoConcepto - 0.01, 2)

                            desglosePrima[posicionSeguro]['montoPrimaNoDev'] = round(desglosePrima[posicionSeguro]['montoPrimaNoDev'] - 0.01, 2)

                            if desglosePrima[posicionSeguro]['montoPrimaNoDev'] == 0:
                                movimiento = movimientoRequestBase.copy()
                                movimiento.update({
                                    'codigo': obtenerCodigoMovimientoPrimaNoDevengada(desglosePrima[posicionSeguro]['tipoOperacion'], 'GASTO_COBRANZA'),
                                    'montoConcepto': montoDescontado,
                                    'ivaConcepto': ivaDescontado,
                                    'tipoOperacion': tipoOperacionDesglose,
                                    'capitalInsoluto': capitalInsolutoRestante,
                                    'numeroPago': noPago,
                                })

                                inputMovimientos.append(movimiento)

                                ivaDescontado = 0.0
                                montoDescontado = 0.0

                                posicionSeguro = posicionSeguro + 1

                            if montoConcepto == 0 and ivaConcepto == 0:
                                isDescontando = False

                    else:
                        movimiento = movimientoRequestBase.copy()
                        movimiento.update({
                            'codigo': codigoMov,
                            'montoConcepto': montoConcepto,
                            'ivaConcepto': ivaConcepto,
                            'reportaCobranza': 'null',
                            'tipoOperacion': tipoOperacionDesglose,
                            'capitalInsoluto': capitalInsolutoRestante,
                            'numeroPago': noPago,
                        })

                        movimientoObj = movimiento.copy()

                        if isDynamicBonification:
                            movimientoObj['subcodigo'] = subcodigo

                        if isBonificacionFondo:
                            movimientoObj['medioPago'] = 'FONDO_PAGOS'

                        inputMovimientos.append(movimientoObj)

            if (interesProyectado != 0.0 or ivaInteresProyectado != 0.0):
                ivaConcepto = 0.0
                montoConcepto = 0.0

                montoConcepto = interesProyectado
                ivaConcepto = ivaInteresProyectado

                codigoMov = ''
                subcodigo = ''

                if 'BONIFICACION' in movPago.get('tipo') and not movPago.get('tipoOperacion') == 'FONDO':
                    if (movPago.get('tipoOperacion') == 'COMISION'):
                        if containsInteresNoContable:
                            codigoMov = 'BCNC'
                        else:
                            codigoMov = 'BINC'
                    if (movPago.get('tipoOperacion') == 'CURA'):
                        if containsInteresNoContable:
                            codigoMov = 'BCVN'
                        else:
                            codigoMov = 'BCVI'
                    if (movPago.get('tipoOperacion') == 'PROMOCION'):
                        if containsInteresNoContable:
                            codigoMov = 'BCIN'
                        else:
                            codigoMov = 'BCIC'
                    if (movPago.get('tipoOperacion')[1:-1] == 'X'):
                        if containsInteresNoContable:
                            codigoMov = 'BNHN'
                        else:
                            codigoMov = 'BNHI'
                        isDynamicBonification = True
                        subcodigo = movPago.get('tipoOperacion')

                    if (movPago.get('tipoOperacion') == 'LIQUIDAR'):
                        if containsInteresNoContable:
                            codigoMov = 'BLVN'
                        else:
                            codigoMov = 'BLVI'
                    if (movPago.get('tipoOperacion') == 'QUITA'):
                        if containsInteresNoContable:
                            codigoMov = 'BQTN'
                        else:
                            codigoMov = 'BQTI'

                    if (movPago.get('tipoOperacion') == 'EXTENSION'):
                        if containsInteresNoContable:
                            codigoMov = 'BENC' if containsInteresSimbolico else 'BERN'
                        else:
                            codigoMov = 'BEIN' if containsInteresSimbolico else 'BERI'

                    if (movPago.get('tipoOperacion') == 'ACLARACION'):
                        if containsInteresNoContable:
                            codigoMov = 'BAIN'
                        else:
                            codigoMov = 'BAIC'
                    if (movPago.get('tipoOperacion') == 'REMANENTE'):
                        if containsInteresNoContable:
                            codigoMov = 'BRNC'
                        else:
                            codigoMov = 'BRIC'
                else:
                    codigoMov = 'PGIN' if not isQuebranto else obtenerCodigoQuebranto(tipoQuebranto, 'INTERESES')

                if isLiquidado and movPago.get('tipoDePago') != 'ATRASADO' and movPago.get('tipo') != 'BONIFICACION' and not isQuebranto:
                    print(f'Agregando montos de liquidacion INTERESES: {montoConcepto}, {ivaConcepto}')

                    montosInteresesLiquidacion.append(montoConcepto)
                    montosIvaInteresesLiquidacion.append(ivaConcepto)
                elif isQuebranto:
                    montosInteresQuebranto.append(montoConcepto)
                    montosIvaInteresQuebranto.append(ivaConcepto)
                else:
                    if isPrimaNoDevengada:
                        isDescontando = True
                        isDescontandoIVA = True

                        while isDescontando:
                            if ivaConcepto == 0:
                                isDescontandoIVA = False

                            if isDescontandoIVA:
                                ivaDescontado = round(ivaDescontado + 0.01, 2)
                                ivaConcepto = round(ivaConcepto - 0.01, 2)
                            else:
                                montoDescontado = round(montoDescontado + 0.01, 2)
                                montoConcepto = round(montoConcepto - 0.01, 2)

                            desglosePrima[posicionSeguro]['montoPrimaNoDev'] = round(desglosePrima[posicionSeguro]['montoPrimaNoDev'] - 0.01, 2)

                            if desglosePrima[posicionSeguro]['montoPrimaNoDev'] == 0:
                                movimiento = movimientoRequestBase.copy()
                                movimiento.update({
                                    'codigo': obtenerCodigoMovimientoPrimaNoDevengada(desglosePrima[posicionSeguro]['tipoOperacion'], 'INTERESES'),
                                    'montoConcepto': montoDescontado,
                                    'ivaConcepto': ivaDescontado,
                                    'tipoOperacion': tipoOperacionDesglose,
                                    'capitalInsoluto': capitalInsolutoRestante,
                                    'numeroPago': noPago,
                                })

                                inputMovimientos.append(movimiento)

                                ivaDescontado = 0.0
                                montoDescontado = 0.0

                                posicionSeguro = posicionSeguro + 1

                            if montoConcepto == 0 and ivaConcepto == 0:
                                isDescontando = False

                    else:
                        movimiento = movimientoRequestBase.copy()
                        movimiento.update({
                            'codigo': codigoMov,
                            'montoConcepto': montoConcepto,
                            'ivaConcepto': ivaConcepto,
                            'tipoOperacion': tipoOperacionDesglose,
                            'capitalInsoluto': capitalInsolutoRestante,
                            'numeroPago': noPago,
                        })
                        movimientoObj = movimiento.copy()

                        if isDynamicBonification:
                            movimientoObj['subcodigo'] = subcodigo

                        if isBonificacionFondo:
                            movimientoObj['medioPago'] = 'FONDO_PAGOS'

                        inputMovimientos.append(movimientoObj)

            if (capital != 0.0 and not isExedente):
                montoConcepto = 0.0
                ivaConcepto = 0.0

                montoConcepto = capital

                codigoMov = ''
                subcodigo = ''

                if 'BONIFICACION' in movPago.get('tipo') and not movPago.get('tipoOperacion') == 'FONDO':
                    if (movPago.get('tipoOperacion') == 'COMISION'):
                        codigoMov = 'BTAC'
                    if (movPago.get('tipoOperacion') == 'CURA'):
                        codigoMov = 'BCVC'
                    if (movPago.get('tipoOperacion') == 'PROMOCION'):
                        codigoMov = 'BCCA'
                    if (movPago.get('tipoOperacion')[1:-1] == 'X'):
                        codigoMov = 'BNHC'
                        isDynamicBonification = True
                        subcodigo = movPago.get('tipoOperacion')

                    if (movPago.get('tipoOperacion') == 'LIQUIDAR'):
                        codigoMov = 'BLVC'
                    if (movPago.get('tipoOperacion') == 'QUITA'):
                        codigoMov = 'BQTC'
                    if (movPago.get('tipoOperacion') == 'EXTENSION'):
                        codigoMov = 'BERC'
                    if (movPago.get('tipoOperacion') == 'ACLARACION'):
                        codigoMov = 'BACA'
                    if (movPago.get('tipoOperacion') == 'REMANENTE'):
                        codigoMov = 'BRCA'
                else:
                    if isAbonoCapital:
                        codigoMov = 'PCAP'
                    else:
                        if movPago.get('tipoOperacion') == 'MONTO':
                            codigoMov = 'BLSM'
                        else:
                            codigoMov = 'PGCA' if not isQuebranto else obtenerCodigoQuebranto(tipoQuebranto, 'CAPITAL')

                if isLiquidado and movPago.get('tipoDePago') != 'ATRASADO' and movPago.get('tipo') != 'BONIFICACION' and not isQuebranto:
                    print(f'Agregando montos de liquidacion CAPITAL: {montoConcepto}, {ivaConcepto}')

                    montosCapitalLiquidacion.append(montoConcepto)
                elif isQuebranto:
                    montosCapitalQuebranto.append(montoConcepto)
                else:
                    if isPrimaNoDevengada:
                        isDescontando = True
                        isDescontandoIVA = True

                        while isDescontando:
                            if ivaConcepto == 0:
                                isDescontandoIVA = False

                            if isDescontandoIVA:
                                ivaDescontado = round(ivaDescontado + 0.01, 2)
                                ivaConcepto = round(ivaConcepto - 0.01, 2)
                            else:
                                montoDescontado = round(montoDescontado + 0.01, 2)
                                montoConcepto = round(montoConcepto - 0.01, 2)

                            desglosePrima[posicionSeguro]['montoPrimaNoDev'] = round(desglosePrima[posicionSeguro]['montoPrimaNoDev'] - 0.01, 2)

                            if desglosePrima[posicionSeguro]['montoPrimaNoDev'] == 0:
                                movimiento = movimientoRequestBase.copy()
                                movimiento.update({
                                    'codigo': obtenerCodigoMovimientoPrimaNoDevengada(desglosePrima[posicionSeguro]['tipoOperacion'], 'GASTO_COBRANZA'),
                                    'montoConcepto': montoDescontado,
                                    'ivaConcepto': ivaDescontado,
                                    'tipoOperacion': tipoOperacionDesglose,
                                    'capitalInsoluto': capitalInsolutoRestante,
                                    'numeroPago': noPago,
                                })

                                inputMovimientos.append(movimiento)

                                ivaDescontado = 0.0
                                montoDescontado = 0.0

                                posicionSeguro = posicionSeguro + 1

                            if montoConcepto == 0 and ivaConcepto == 0:
                                isDescontando = False

                    elif isUltimoPago and isBonificacionSaldoMinimo and not contieneBonificacion:
                        montoConcepto = montoPago

                        # Movimiento con menos centavos
                        movimientoMontos = movimientoRequestBase.copy()
                        movimientoMontos.update({
                            'codigo': codigoMov,
                            'montoConcepto': montoConcepto,
                            'ivaConcepto': ivaConcepto,
                            'tipoOperacion': tipoOperacionDesglose,
                            'capitalInsoluto': capitalInsolutoRestante,
                            'numeroPago': noPago,
                        })

                        inputMovimientos.append(movimientoMontos)

                        movimientoDiferencia = movimientoRequestBase.copy()
                        movimientoDiferencia.update({
                            'codigo': 'BLSM',
                            'montoConcepto': diferenciaPago,
                            'ivaConcepto': ivaConcepto,
                            'tipoOperacion': tipoOperacionDesglose,
                            'capitalInsoluto': capitalInsolutoRestante,
                            'numeroPago': noPago,
                        })

                        inputMovimientos.append(movimientoDiferencia)

                    else:
                        movimiento = movimientoRequestBase.copy()
                        movimiento.update({
                            'codigo': codigoMov,
                            'montoConcepto': montoConcepto,
                            'ivaConcepto': ivaConcepto,
                            'tipoOperacion': tipoOperacionDesglose,
                            'capitalInsoluto': capitalInsolutoRestante,
                            'numeroPago': noPago,
                        })

                        movimientoObj = movimiento.copy()

                        if isDynamicBonification:
                            movimientoObj['subcodigo'] = subcodigo

                        if isBonificacionFondo:
                            movimientoObj['medioPago'] = 'FONDO_PAGOS'

                        inputMovimientos.append(movimientoObj)

            if (capital != 0.0 and isExedente):
                montoConcepto = 0.0
                ivaConcepto = 0.0

                montoConcepto = abs(capital)

                codigoMov = 'PGSF'

                hasSaldoFavor = True
                montoSaldoFavor = montoSaldoFavor + montoConcepto

                if isPrimaNoDevengada:

                    isDescontando = True
                    isDescontandoIVA = True

                    while isDescontando:
                        if ivaConcepto == 0:
                            isDescontandoIVA = False

                        if isDescontandoIVA:
                            ivaDescontado = round(ivaDescontado + 0.01, 2)
                            ivaConcepto = round(ivaConcepto - 0.01, 2)
                        else:
                            montoDescontado = round(montoDescontado + 0.01, 2)
                            montoConcepto = round(montoConcepto - 0.01, 2)

                        desglosePrima[posicionSeguro]['montoPrimaNoDev'] = round(desglosePrima[posicionSeguro]['montoPrimaNoDev'] - 0.01, 2)

                        if desglosePrima[posicionSeguro]['montoPrimaNoDev'] == 0:
                            print(f"Estado seguros: {desglosePrima}, posicion seguros: {posicionSeguro}")
                            movimiento = movimientoRequestBase.copy()
                            movimiento.update({
                                'codigo': obtenerCodigoMovimientoPrimaNoDevengada(desglosePrima[posicionSeguro]['tipoOperacion'], 'SALDO_FAVOR'),
                                'montoConcepto': montoDescontado,
                                'ivaConcepto': ivaDescontado,
                                'tipoOperacion': tipoOperacionDesglose,
                                'capitalInsoluto': capitalInsolutoRestante,
                                'numeroPago': noPago,
                            })

                            inputMovimientos.append(movimiento)

                            ivaDescontado = 0.0
                            montoDescontado = 0.0

                            posicionSeguro = posicionSeguro + 1

                        if montoConcepto == 0 and ivaConcepto == 0:
                            isDescontando = False

                else:
                    print('Crearía movimiento saldo favor')
                    tipoOperacionDesgloseSaldoFavor = tipoOperacionDesglose
                    capitalInsolutoRestanteSaldoFavor = capitalInsolutoRestante
                    movimiento = movimientoRequestBase.copy()
                    movimiento.update({
                        'codigo': codigoMov,
                        'montoConcepto': abs(montoConcepto),
                        'ivaConcepto': abs(ivaConcepto),
                        'tipoOperacion': tipoOperacionDesglose,
                        'capitalInsoluto': capitalInsolutoRestante,
                        'tipoOperacion': tipoOperacionDesglose,
                        'numeroPago': noPago,
                    })
                    inputMovimientos.append(movimiento)

        if isLiquidado and not isQuebranto:

            gastoCobranzaLiquidacion = fsum(montosGCLiquidacion)
            ivaGastoCobranzaLiquidacion = fsum(montosIvaGCLiquidacion)

            interesLiquidacion = fsum(montosInteresesLiquidacion)
            ivaInteresLiquidacion = fsum(montosIvaInteresesLiquidacion)

            capitalLiquidacion = fsum(montosCapitalLiquidacion)

            logMontosLiquidacion = {
                'gastoCobranza': gastoCobranzaLiquidacion,
                'ivaGastoCobranza': ivaGastoCobranzaLiquidacion,
                'interes': interesLiquidacion,
                'ivaInteres': ivaInteresLiquidacion,
                'capital': capitalLiquidacion
            }

            print(f'Montos liquidacion: {json.dumps(logMontosLiquidacion)}')

            if (len(montosGCLiquidacion) != 0):
                movimiento = movimientoRequestBase.copy()
                movimiento.update({
                    'numeroPago': 0,
                    'codigo': 'PPGC' if isPrimaNoDevengada else obtenerCodigoQuebranto(tipoQuebranto, 'GASTO_COBRANZA') if isQuebranto else 'PGGC',
                    'montoConcepto': gastoCobranzaLiquidacion,
                    'ivaConcepto': ivaGastoCobranzaLiquidacion,
                    'tipoOperacion': tipoOperacionDesglose,
                    'capitalInsoluto': capitalInsolutoRestante
                })

                inputMovimientos.append(movimiento)
            if (len(montosInteresesLiquidacion) != 0):
                movimiento = movimientoRequestBase.copy()
                movimiento.update({
                    'numeroPago': 0,
                    'codigo': 'PPIN' if isPrimaNoDevengada else obtenerCodigoQuebranto(tipoQuebranto, 'INTERESES') if isQuebranto else 'PGIN',
                    'montoConcepto': interesLiquidacion,
                    'ivaConcepto': ivaInteresLiquidacion,
                    'tipoOperacion': tipoOperacionDesglose,
                    'capitalInsoluto': capitalInsolutoRestante
                })

                inputMovimientos.append(movimiento)
            if (len(montosCapitalLiquidacion) != 0):

                if isBonificacionSaldoMinimo and not contieneBonificacion:
                    movimientoCapital = movimientoRequestBase.copy()
                    movimientoCapital.update({
                        'numeroPago': 0,
                        'codigo': 'PPCA' if isPrimaNoDevengada else obtenerCodigoQuebranto(tipoQuebranto, 'CAPITAL') if isQuebranto else 'PGCA',
                        'montoConcepto': round(capitalLiquidacion - diferenciaPago, 2),
                        'ivaConcepto': 0.0,
                        'tipoOperacion': tipoOperacionDesglose,
                        'capitalInsoluto': capitalInsolutoRestante
                    })
                    inputMovimientos.append(movimientoCapital)

                    movimientoDiferencia = movimientoRequestBase.copy()
                    movimientoDiferencia.update({
                        'numeroPago': 0,
                        'codigo': 'BLSM',
                        'montoConcepto': round(diferenciaPago, 2),
                        'ivaConcepto': 0.0,
                        'tipoOperacion': tipoOperacionDesglose,
                        'capitalInsoluto': capitalInsolutoRestante
                    })

                    inputMovimientos.append(movimientoDiferencia)
                else:
                    movimiento = movimientoRequestBase.copy()
                    movimiento.update({
                        'numeroPago': 0,
                        'codigo': 'PPCA' if isPrimaNoDevengada else obtenerCodigoQuebranto(tipoQuebranto, 'CAPITAL') if isQuebranto else 'PGCA',
                        'montoConcepto': capitalLiquidacion,
                        'ivaConcepto': 0.0,
                        'tipoOperacion': tipoOperacionDesglose,
                        'capitalInsoluto': capitalInsolutoRestante
                    })

                    inputMovimientos.append(movimiento)
        elif isLiquidado and isQuebranto:
            gastoCobranzaQuebrato = round(fsum(montosGCQuebranto), 2)
            ivaGastoCobranzaQuebranto = round(fsum(montosIvaGCQuebranto), 2)

            interesQuebranto = round(fsum(montosInteresQuebranto), 2)
            ivaInteresQuebranto = round(fsum(montosIvaInteresQuebranto), 2)

            capitalQuebranto = round(fsum(montosCapitalQuebranto), 2)

            logMontosQuebranto = {
                'gastoCobranza': gastoCobranzaQuebrato,
                'ivaGastoCobranza': ivaGastoCobranzaQuebranto,
                'interes': interesQuebranto,
                'ivaInteres': ivaInteresQuebranto,
                'capital': capitalQuebranto
            }

            print(f'Montos quebranto: {logMontosQuebranto}')

            if (len(montosGCQuebranto) != 0):
                movimiento = movimientoRequestBase.copy()
                movimiento.update({
                    'numeroPago': 0,
                    'codigo': obtenerCodigoQuebranto(tipoQuebranto, 'GASTO_COBRANZA'),
                    'montoConcepto': gastoCobranzaQuebrato,
                    'ivaConcepto': ivaGastoCobranzaQuebranto,
                    'tipoOperacion': tipoOperacionDesglose,
                    'capitalInsoluto': capitalInsolutoRestante
                })

                inputMovimientos.append(movimiento)
            if (len(montosInteresQuebranto) != 0):
                movimiento = movimientoRequestBase.copy()
                movimiento.update({
                    'numeroPago': 0,
                    'codigo': obtenerCodigoQuebranto(tipoQuebranto, 'INTERESES'),
                    'montoConcepto': interesQuebranto,
                    'ivaConcepto': ivaInteresQuebranto,
                    'tipoOperacion': tipoOperacionDesglose,
                    'capitalInsoluto': capitalInsolutoRestante
                })

                inputMovimientos.append(movimiento)
            if (len(montosCapitalQuebranto) != 0):
                movimiento = movimientoRequestBase.copy()
                movimiento.update({
                    'numeroPago': 0,
                    'codigo': obtenerCodigoQuebranto(tipoQuebranto, 'CAPITAL'),
                    'montoConcepto': capitalQuebranto,
                    'ivaConcepto': 0,
                    'tipoOperacion': tipoOperacionDesglose,
                    'capitalInsoluto': capitalInsolutoRestante
                })

                inputMovimientos.append(movimiento)
    elif isPagoClienteCumplido:
        movimientoClienteCumplido = movimientoRequestBase.copy()
        movimientoClienteCumplido.update({
            'numeroPago': 0,
            'codigo': 'SFCC',
            'montoConcepto': montoPago,
            'ivaConcepto': 0,
            'tipoOperacion': tipoOperacion,
            'capitalInsoluto': 0
        })
        inputMovimientos.append(movimientoClienteCumplido)

    elif isSaldoFavorFondo:
        movimientoSaldoFavorFondo = movimientoRequestBase.copy()
        movimientoSaldoFavorFondo.update({
            'numeroPago': 0,
            'codigo': 'SFFP',
            'montoConcepto': montoPago,
            'ivaConcepto': 0,
            'tipoOperacion': tipoOperacion,
            'capitalInsoluto': 0
        })
        inputMovimientos.append(movimientoSaldoFavorFondo)

    # =============== MOVIMIENTOS ===============

    if hasSaldoFavor and not isPrimaNoDevengada and not isLiquidado:
        movimiento = movimientoRequestBase.copy()
        movimiento.update({
            'numeroPago': 0,
            'codigo': 'SFCC' if isPagoClienteCumplido else 'PGSF',
            'montoConcepto': abs(montoSaldoFavor),
            'ivaConcepto': abs(ivaConcepto),
            'tipoOperacion': tipoOperacionDesgloseSaldoFavor,
            'capitalInsoluto': capitalInsolutoRestanteSaldoFavor
        })
        inputMovimientos.append(movimiento)

    if isPrimaNoDevengada:
        for desgloseSeguro in desglosePrima:
            movimiento = movimientoRequestBase.copy()
            movimiento.update({
                'numeroPago': 0,
                'codigo': obtenerCodigoMovimientoPrimaNoDevengada(tipoOperacion=desgloseSeguro.get('tipoOperacion'), rubro='SALDO_FAVOR'),
                'montoConcepto': desgloseSeguro.get('montoPrimaNoDev'),
                'ivaConcepto': 0,
                'tipoOperacion': tipoOperacion
            })
            inputMovimientos.append(movimiento)

    statusRequestMovimientos = None
    responseMovimientos = None
    responseMovimientosJson = None

    if isBonificacionExtension:
        movimientoExtension = {
            'idDisposicion': idDisposicion,
            'idLineaCredito': lineaCredito,
            'contrato': contrato,
            'idTenant': claveEmpresa,
            'idTransaccion': idTransaccion,
            'sucursalCartera': sucursalCartera,
            'sucursalOrigen': sucursalOrigen,
            'fechaContable': str(dtFechaContable.date()),
            'fechaValor': str(dtFechaValor.date()),
            'reportaCobranza': 'null',
            'operador': operador,
            'origenPago': origenPago,
            'medioPago': medioPago,
            'numeroPago': 0,
            'codigo': 'EXPG',
            'montoConcepto': round(cargoCapitalBonificacionLedger + cargoInteresBonificacionLedger - cargoInteresSimbolicoLedger, 2),
            'ivaConcepto': round(cargoIvaInteresBonificacionLedger - cargoIvaInteresSimbolicoLedger, 2),
            'tipoOperacion': tipoOperacion,
            'capitalInsulto': saldoFinalExtension
        }

        inputMovimientos.append(movimientoExtension)

    if not hasError:

        urlMovimientos = 'http://cr-movimientos-service-v1-stable.new-core.svc.cluster.local/v1/movimientos/'

        try:

            responseMovimientos = requests.post(url=urlMovimientos, json=inputMovimientos)
            statusRequestMovimientos = responseMovimientos.status_code
            responseMovimientosJson = responseMovimientos.json()

            print('URL: {}\nRequest: {}\nResponse: {}\nEstatus: {}'.format(urlMovimientos, inputMovimientos, json.dumps(responseMovimientosJson), str(statusRequestMovimientos)))
        except Exception as e:
            print('Error llamando movimientos: {}'.format(e))
            statusRequestMovimientos = 500

    if statusRequestMovimientos != 200 and not hasError:
        hasError = True
        fase = 'MOVIMIENTOS'

        errorRequest = inputMovimientos
        errorResponse = responseMovimientosJson

    # =============== LEDGER ===============

    if not hasError and aplicaLedger:

        if isBonificacionSaldoMinimo and not contieneBonificacion:
            cargoCapitalLedger = cargoCapitalLedger - diferenciaPago
            cargoCapitalBonificacionLedger = diferenciaPago

        pagoLedger = {
            'capital': cargoCapitalLedger,
            'interes': cargoInteresLedger,
            'ivaInteres': cargoIvaInteresLedger,
            'interesNoContable': cargoInteresNoContableLedger,
            'ivaInteresNoContable': cargoIvaInteresNoContableLedger,
            'gc': cargoCobranzaLedger,
            'ivaGc': cargoIvaCobranzaLedger,
            'saldoFavor': cargoSaldoAFavor
        }

        bonificacionLedger = {
            'capital': cargoCapitalBonificacionLedger,
            'interes': cargoInteresBonificacionLedger,
            'ivaInteres': cargoIvaInteresBonificacionLedger,
            'interesNoContable': cargoInteresNoContableBonificacionLedger,
            'ivaInteresNoContable': cargoIvaInteresNoContableBonificacionLedger,
            'gc': cargoCobranzaBonificacionLedger,
            'ivaGc': cargoIvaCobranzaBonificacionLedger
        }

        bonificacionSimbolicaLedger = {
            'interes': cargoInteresSimbolicoLedger,
            'ivaInteres': cargoIvaInteresSimbolicoLedger
        }

        fondoLedger = {
            'capital': cargoCapitalLedgerFondo,
            'interes': cargoInteresFondo,
            'ivaInteres': cargoIvaInteresesFondo,
            'gc': cargoCobranzaFondo,
            'ivaGc': cargoIvaCobranzaFondo
        }

        print(f'Montos pago: {json.dumps(pagoLedger)}')
        print(f'Montos bonificación: {json.dumps(bonificacionLedger)}')
        print(f'Montos simbolico: {json.dumps(bonificacionSimbolicaLedger)}')
        print(f'Montos fondo: {json.dumps(fondoLedger)}')

        # CONCEPTOS PAGO LEDGER
        if (cargoCapitalLedger != 0):
            operacionesledger.append({
                'monto': round(cargoCapitalLedger, 2),
                'concepto': 'capital'
            })

        if (cargoInteresLedger != 0):
            operacionesledger.append({
                'monto': round(cargoInteresLedger, 2),
                'concepto': 'intereses'
            })

        if (cargoIvaInteresLedger != 0):
            if (cargoInteresLedger == 0):
                operacionesledger.append({
                    'monto': 0.0,
                    'concepto': 'intereses'
                })
            operacionesledger.append({
                'monto': round(cargoIvaInteresLedger, 2),
                'concepto': 'iva_intereses'
            })

        if (cargoCobranzaLedger != 0):
            operacionesledger.append({
                'monto': round(cargoCobranzaLedger, 2),
                'concepto': 'gastos_cobranza'
            })

        if (cargoIvaCobranzaLedger != 0):
            if (cargoCobranzaLedger == 0):
                operacionesledger.append({
                    'monto': 0.0,
                    'concepto': 'gastos_cobranza'
                })
            operacionesledger.append({
                'monto': round(cargoIvaCobranzaLedger, 2),
                'concepto': 'iva_gastos_cobranza'
            })

        if (cargoSaldoAFavor != 0):
            operacionesledger.append({
                'monto': abs(round(cargoSaldoAFavor, 2)),
                'concepto': 'saldo_a_favor'
            })

        # CONCEPTOS BONIFICACION LEDGER
        if (cargoCapitalBonificacionLedger != 0) and not isBonificacionExtension:
            operacionesBonificacionLedger.append({
                'monto': round(cargoCapitalBonificacionLedger, 2),
                'concepto': 'capital'
            })

        interesExtension = cargoInteresBonificacionLedger
        ivaInteresExtension = cargoIvaInteresBonificacionLedger

        cargoInteresBonificacionLedger = cargoInteresSimbolicoLedger if isBonificacionExtension else cargoInteresBonificacionLedger
        if (cargoInteresBonificacionLedger != 0):
            operacionesBonificacionLedger.append({
                'monto': round(cargoInteresBonificacionLedger, 2),
                'concepto': 'intereses'
            })

        cargoIvaInteresBonificacionLedger = ivaInteresSimbolico if isBonificacionExtension else cargoIvaInteresBonificacionLedger
        if (cargoIvaInteresBonificacionLedger != 0):
            if (cargoInteresBonificacionLedger == 0):
                operacionesBonificacionLedger.append({
                    'monto': 0.0,
                    'concepto': 'intereses'
                })

            operacionesBonificacionLedger.append({
                'monto': round(cargoIvaInteresBonificacionLedger, 2),
                'concepto': 'iva_intereses'
            })

        if cargoInteresNoContableLedger != 0 or cargoIvaInteresNoContableLedger != 0:
            cargoInteresNoContableBonificacionLedger = round(cargoInteresNoContableBonificacionLedger + cargoInteresNoContableLedger, 2)
            cargoIvaInteresNoContableBonificacionLedger = round(cargoIvaInteresNoContableBonificacionLedger + cargoIvaInteresNoContableLedger, 2)

        if (cargoInteresNoContableBonificacionLedger != 0):
            operacionesBonificacionLedger.append({
                'monto': round(cargoInteresNoContableBonificacionLedger, 2),
                'concepto': 'intereses_no_contables'
            })

        if (cargoIvaInteresNoContableBonificacionLedger != 0):
            operacionesBonificacionLedger.append({
                'monto': round(cargoIvaInteresNoContableBonificacionLedger, 2),
                'concepto': 'iva_intereses_no_contables'
            })

        if (cargoCobranzaBonificacionLedger != 0):
            operacionesBonificacionLedger.append({
                'monto': round(cargoCobranzaBonificacionLedger, 2),
                'concepto': 'gastos_cobranza'
            })

        if (cargoIvaCobranzaBonificacionLedger != 0):
            if (cargoCobranzaBonificacionLedger == 0):
                operacionesBonificacionLedger.append({
                    'monto': 0.0,
                    'concepto': 'gastos_cobranza'
                })
            operacionesBonificacionLedger.append({
                'monto': round(cargoIvaCobranzaBonificacionLedger, 2),
                'concepto': 'iva_gastos_cobranza'
            })

        print('Operaciones Ledger: {}'.format(operacionesledger))
        print('Operaciones Bonificaciones Ledger: {}'.format(operacionesBonificacionLedger))

        if (medioPago == 'OXXOLINEA' or medioPago == 'OXXL'):
            medioPago = 'OXXO'

        isAgregaCargo = False
        if origenPago == 'PAGO_PND':
            origenPago = 'NEWCORE'
            medioPago = 'CANCELACION_SEGUROS'
            isAgregaCargo = True

        if isAgregaCargo:
            inputLedger.append({
                'claveEmpresa': claveEmpresa,
                'contrato': contrato,
                'idDisposicion': idDisposicion,
                'estatusCartera': estatusCartera,
                'etapaCarteraDiario': etapaCarteraDiario,
                'origenPago': origenPago,
                'medioPago': medioPago,
                'centroCostos': centroCostos,
                'ejecucion': 'CARGO',
                'tipoOperacion': tipoOperacion,
                'fechaValor': str(dtFechaValor.date()),
                'idTransaccion': idTransaccion,
                'referencia': referencia,
                'propietario': propietario,
                'operaciones': [{
                    'monto': montoPago,
                    'concepto': 'MONTO_PAGO'
                }]
            })

        if isPrimaNoDevengada:
            print('RECORRIENDO DESGLOSE PRIMAS NO DEVENGADAS PARA LEDGER')
            for prima in desglosePrimaForLedger:
                tipoOperacionPrima = '{}_PRIMA_NO_DEV'.format(prima.get('tipoOperacion'))
                montoPrima = prima.get('montoPrimaNoDev')

                inputLedger.append({
                    'claveEmpresa': claveEmpresa,
                    'contrato': contrato,
                    'idDisposicion': idDisposicion,
                    'estatusCartera': estatusCartera,
                    'etapaCarteraDiario': etapaCarteraDiario,
                    'origenPago': origenPago,
                    'medioPago': medioPago,
                    'centroCostos': centroCostos,
                    'ejecucion': 'AMBOS',
                    'tipoOperacion': tipoOperacionPrima,
                    'fechaValor': str(dtFechaValor.date()),
                    'idTransaccion': idTransaccion,
                    'propietario': propietario,
                    'referencia': referencia,
                    'operaciones': [
                        {
                            'concepto': 'CAPITAL',
                            'monto': montoPrima
                        }
                    ]
                })

            if origenPago == 'NEWCORE' and isLiquidado:
                print('AGREGANDO ABONOS CANCELACION DIRECTA DE SEGURO')
                inputLedger.append({
                    'claveEmpresa': claveEmpresa,
                    'contrato': contrato,
                    'idDisposicion': idDisposicion,
                    'estatusCartera': estatusCartera,
                    'etapaCarteraDiario': etapaCarteraDiario,
                    'origenPago': origenPago,
                    'medioPago': medioPago,
                    'centroCostos': centroCostos,
                    'ejecucion': 'ABONO',
                    'tipoOperacion': tipoOperacion,
                    'fechaValor': str(dtFechaValor.date()),
                    'idTransaccion': idTransaccion,
                    'propietario': propietario,
                    'referencia': referencia,
                    'operaciones': operacionesledger
                })
        else:
            if not isPagoClienteCumplido:
                if isPagoFondo and montoFondoPago == 0:
                    operacionesFondoPago = []
                    if isSaldoFavorFondo:
                        operacionesFondoPago.append({
                            'monto': montoPago,
                            'concepto': 'SALDO_A_FAVOR'
                        })
                    else:
                        operacionesFondoPago.append({
                            'monto': montoPago,
                            'concepto': 'CAPITAL'
                        })

                print('AGREGANDO ABONO PAGO')

                inputLedger.append({
                    'claveEmpresa': claveEmpresa,
                    'contrato': contrato,
                    'idDisposicion': idDisposicion,
                    'estatusCartera': estatusCartera,
                    'etapaCarteraDiario': etapaCarteraDiario,
                    'origenPago': origenPago,
                    'medioPago': medioPago,
                    'centroCostos': centroCostos,
                    'ejecucion': 'ABONO',
                    'tipoOperacion': 'PAGO_ASEGURADORA' if isPagoSeguro else obtenerTipoOperacionLedgerQuebranto(tipoQuebranto) if isQuebranto else tipoOperacion,
                    'fechaValor': str(dtFechaValor.date()),
                    'idTransaccion': idTransaccion,
                    'propietario': propietario,
                    'referencia': referencia,
                    'operaciones': operacionesledger
                })

        if (len(operacionesBonificacionLedger) != 0):
            print('AGREGANDO BONIFICACIONES LEDGER')

            ejecucionBonifFondo = 'ABONO' if isMontoFondoPagoDifZero else 'AMBOS'

            inputLedger.append({
                'claveEmpresa': claveEmpresa,
                'contrato': contrato,
                'idDisposicion': idDisposicion,
                'estatusCartera': estatusCartera,
                'etapaCarteraDiario': etapaCarteraDiario,
                'origenPago': origenPago,
                'medioPago': 'FONDO_PAGOS' if isMontoFondoPagoDifZero else medioPago,
                'centroCostos': centroCostos,
                'ejecucion': ejecucionBonifFondo,
                'tipoOperacion': 'BONIFICACION_EXTENSION_PAGO' if isBonificacionExtension else tipoOperacion if isMontoFondoPagoDifZero else 'BONIFICACION_{}'.format(tipoOperacion),
                'fechaValor': str(dtFechaValor.date()),
                'idTransaccion': idTransaccion,
                'referencia': referencia,
                'propietario': propietario,
                'operaciones': operacionesBonificacionLedger
            })

            if isBonificacionExtension:
                operacionesPagoExtension = [
                    {
                        'concepto': 'capital',
                        'monto': round(cargoCapitalBonificacionLedger, 2)
                    }
                ]

                if cargoInteresBonificacionLedger != 0:
                    operacionesPagoExtension.append({
                        'concepto': 'intereses',
                        'monto': round(interesExtension - interesSimbolico, 2)
                    })

                if cargoIvaInteresBonificacionLedger != 0:
                    operacionesPagoExtension.append({
                        'concepto': 'iva_intereses',
                        'monto': round(ivaInteresExtension - ivaInteresSimbolico, 2)
                    })

                print('Agregando CARGO/ABONO extension pago')
                inputLedger.append({
                    'claveEmpresa': claveEmpresa,
                    'contrato': contrato,
                    'idDisposicion': idDisposicion,
                    'estatusCartera': estatusCartera,
                    'etapaCarteraDiario': etapaCarteraDiario,
                    'origenPago': origenPago,
                    'medioPago': medioPago,
                    'centroCostos': centroCostos,
                    'ejecucion': 'AMBOS',
                    'tipoOperacion': 'EXTENSION_PAGO',
                    'fechaValor': str(dtFechaValor.date()),
                    'idTransaccion': idTransaccion,
                    'referencia': referencia,
                    'propietario': propietario,
                    'operaciones': operacionesPagoExtension
                })

        urlLedger = 'http://cr-ledger-service-v1-stable.new-core.svc.cluster.local/v1/ledger/aplicarOperaciones'

        try:
            responseMovimientosLedger = requests.post(url=urlLedger, json=inputLedger)
            estatusResponseLedger = responseMovimientosLedger.status_code
            responseLedgerJson = responseMovimientosLedger.json()

            print('URL: {}\nRequest: {}\nResponse: {}\nEstatus: {}'.format(urlLedger, inputLedger, json.dumps(responseLedgerJson), str(estatusResponseLedger)))
        except:
            print('Error llamando ledger')
            estatusResponseLedger = 500
            hasError = True
            fase = 'LEDGER'

        if estatusResponseLedger != 201 and not hasError:
            hasError = True
            fase = 'LEDGER'

            errorRequest = inputLedger
            errorResponse = responseLedgerJson

        inputPagosLedger = []

        if not isPrimaNoDevengada:
            print('PERFORMANCE | AGREGANDO CARGO LEDGER')
            inputPagosLedger.append({
                'claveEmpresa': claveEmpresa,
                'contrato': contrato,
                'idDisposicion': idDisposicion,
                'estatusCartera': estatusCartera,
                'etapaCarteraDiario': etapaCarteraDiario,
                'origenPago': origenPago,
                'medioPago': medioPago,
                'centroCostos': centroCostos,
                'ejecucion': 'CARGO',
                'tipoOperacion': tipoOperacion,
                'fechaValor': str(dtFechaValor.date()),
                'idTransaccion': idTransaccion,
                'referencia': referencia,
                'propietario': propietario,
                'operaciones': [{
                    'monto': montoPago,
                    'concepto': 'MONTO_PAGO'
                }]
            })

        inputPagosLedger.append(inputLedger[0] if len(inputLedger) != 0 else None)

        inputLedger = inputPagosLedger
        kwargs['ti'].xcom_push(key='inputPagosLedger', value=inputPagosLedger)

        if len(operacionesBonificacionLedger) != 0:
            hasBonificacion = True
            kwargs['ti'].xcom_push(key='inputBonifsLedger', value=[inputLedger[1]])
        else:
            kwargs['ti'].xcom_push(key='inputBonifsLedger', value=None)

    # =============== SALDOS ===============

    urlSaldosTotales = 'http://core-saldos-service-v1-stable.new-core.svc.cluster.local/v1/saldos/totales/{}'.format(str(lineaCredito))
    responseSaldosTotales = None
    statusRequest = None
    responseSaldosTotalesJson = None

    if not hasError:
        try:
            responseSaldosTotales = requests.put(url=urlSaldosTotales, json={})
            statusRequest = responseSaldosTotales.status_code
            responseSaldosTotalesJson = responseSaldosTotales.json()

            print('URL: {}\nResponse: {}\nEstatus: {}'.format(urlSaldosTotales, json.dumps(responseSaldosTotalesJson), str(statusRequest)))
        except:
            print('Error llamando saldos-totales')
            statusRequest = 500

    if statusRequest != 200 and not hasError:
        hasError = True
        fase = 'SALDOS'

        errorRequest = urlSaldosTotales
        errorResponse = responseSaldosTotalesJson

    isLiquidacionAnticipada = False
    if isLiquidado and noPagosCubiertos > 1:
        isLiquidacionAnticipada = True

    if not isLiquidado and desglosePrima != None and medioPago == 'CANCELACION_SEGUROS':
        isLiquidado = True

    # =============== NOTIFICACION KAFKA ===============

    idBitacoraBonificacion = 0

    print('Pago hasError {} on fase {}'.format(str(hasError), fase))

    if not hasError:
        print('Llamando Kafka: Exito')

        inputKafka = {
            'liquidado': isLiquidado,
            'liquidacionAnticipada': isLiquidacionAnticipada,
            'bonificacionComision': isBonificacionComision,
            'primaNoDevengada': isPrimaNoDevengada,
            'status': 'A',
            'mensaje': 'EXITO',
            'idBitacora': idBitacora,
            'idBitacoraSeguros': idBitacoraSeguros,
            'idBitacoraPagoDevSeguro': idBitacoraPagoDevSeguro,
            'idBitacoraBonif': idBitacoraBonificacion,
            'peticionLedgerPago': inputLedger,
            'peticionLedgerBonificacion': inputLedger[1] if hasBonificacion else None,
            'liquidacionSeguro': liquidacionSeguro,
            'operacion': operacion,
            'propietario': propietario,
            'aplicoBonificacion': False if isMontoFondoPagoDifZero else hasBonificacion,
            'aplicoQuebranto': isQuebranto
        }

        if hasBonificacion:
            saldoCapital = cargoCapitalBonificacionLedger
            saldoGastosCobranza = sum([cargoCobranzaBonificacionLedger, cargoIvaCobranzaBonificacionLedger])
            saldoOrdinarios = sum([cargoInteresBonificacionLedger, cargoIvaInteresBonificacionLedger])

            inputKafka['saldoCapital'] = saldoCapital
            inputKafka['saldoGastosCobranza'] = saldoGastosCobranza
            inputKafka['saldoOrdinarios'] = saldoOrdinarios

        requestKafka = requests.post(url='http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core', json=inputKafka)
        statusCodeKafka = requestKafka.status_code

        print('Request: {}\nEstatus: {}'.format(inputKafka, str(statusCodeKafka)))
    else:
        cancelaCargoLedger = True if fase in ['BONIFICACION_COMISION', 'PAGOS', 'MOVIMIENTOS'] else False

        error = {
            'BONIFICACION_COMISION': 'bonificacion comision',
            'BONIFICACION_FONDO': 'bonificacion fondo',
            'PAGOS': 'pagos',
            'MOVIMIENTOS': 'movimientos',
            'LEDGER': 'ledger',
            'SALDOS': 'saldos totales',
            'FONDO': 'fondos'
        }

        errMsg = error.get(fase)

        kwargs['ti'].xcom_push(key='idBitacoraSeguros', value=idBitacoraSeguros)
        kwargs['ti'].xcom_push(key='idBitacora', value=idBitacora)
        kwargs['ti'].xcom_push(key='idDisposicion', value=idDisposicion)
        kwargs['ti'].xcom_push(key='idLineaCredito', value=lineaCredito)
        kwargs['ti'].xcom_push(key='referencia', value=referencia)
        kwargs['ti'].xcom_push(key='idTransaccion', value=idTransaccion)
        kwargs['ti'].xcom_push(key='fechaValor', value=str(dtFechaValor.date()))

        kwargs['ti'].xcom_push(key='fase', value=fase)
        kwargs['ti'].xcom_push(key='hasError', value=hasError)
        kwargs['ti'].xcom_push(key='errMsg', value=errMsg)
        kwargs['ti'].xcom_push(key='errorResponse', value=errorResponse)

        print('Publicando error')
        mensaje = 'Ocurrio un error al invocar el servicio {} | {}'.format(errMsg, errorResponse)

        inputKafka = {
            'errorRequest': errorRequest,
            'errorResponse': errorResponse,
            'status': 'E',
            'mensaje': mensaje[:254],
            'idBitacora': idBitacora,
            'idBitacoraSeguros': idBitacoraSeguros,
            'idBitacoraPagoDevSeguro': idBitacoraPagoDevSeguro,
            'operacion': operacion,
            'cancelaCargoLedger': cancelaCargoLedger
        }

        requestKafka = requests.post(url='http://cr-producer-airflow-service-v1-stable.new-core.svc.cluster.local/v1/producer-airflow/new-core-error', json=inputKafka)
        statusCodeKafka = requestKafka.status_code
        print('Request: {}\nEstatus: {}'.format(inputKafka, str(statusCodeKafka)))

    # =============== REAPLICACION ===============
    if len(pagosReaplicar) != 0 and not hasError:
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

    if hasError:
        return 'saga_call_task'
    else:
        raise AirflowSkipException('El pago no tuvo error')


class Motivo(Enum):
    ACTIVACION_BONIFICACION = 'activacion de bonificacion por parcialidades'
    INTERMEDIO = 'aplicaicon de pago entre fechas'


def pagosPosteriores(pagoList, strFechaValor):
    print(f'Buscando pagos a fecha valor posteriores a: {strFechaValor}')
    dtFechaValor = datetime.strptime(strFechaValor, "%Y-%m-%d %H:%M:%S").date()
    pagos = [pago for pago in pagoList if datetime.fromisoformat(pago["fechaValor"]).date() > dtFechaValor]
    return pagos


def obtenerPagosEnFecha(pagos: list, dtAplicacionBonif: datetime, dtFechaLimite: datetime):
    pagosEnFecha = []

    print(f'Realizando el filtro de {len(pagos)} pagos aplicados entre la fecha {dtAplicacionBonif.date()}|{dtFechaLimite.date()}')
    for pago in pagos:
        dtFechaOperacion = datetime.strptime(pago.get('fechaOperacion'), '%Y-%m-%d %H:%M:%S')

        isPagoEnFecha = dtAplicacionBonif.date() <= dtFechaOperacion.date() <= dtFechaLimite.date()

        if isPagoEnFecha and pago.get('status') == 'A':
            if dtFechaOperacion.time() >= dtAplicacionBonif.time():
                print(
                    f'Pago con bitácora dentro de fecha {isPagoEnFecha} con {pago.get("idBitacoraPago")} aplicado el: {dtFechaOperacion.date()} {dtFechaOperacion.time()} dentro del tiempo de aplicacion {dtAplicacionBonif.date()} {dtAplicacionBonif.time()}')
                pagosEnFecha.append(pago)

    return pagosEnFecha


def cancelarPagosAnteriores(pagos, motivo: Motivo):
    statusCancelacion = []

    pagos = sorted(pagos, key=lambda x: x.get('idBitacoraPago'), reverse=False)

    for pago in pagos[::-1]:
        try:
            urlReversaPago = f'http://cr-aplica-pago-service-v1-stable.new-core.svc.cluster.local/v1/pagos/{pago.get("idBitacoraPago")}'

            ref = datetime.now().strftime("%Y%m%d%H%M%S")
            mensajeCancelacion = motivo.value

            inputReversarPago = {
                'medioDeCancelacion': pago.get('medioPago'),
                'mensajeCancelacion': 'Cancelado por ' + mensajeCancelacion,
                'referenciaCancelacion': ref,
                'cancelarUnico': False
            }

            requests.post(url=urlReversaPago, json=inputReversarPago)

            print(f'Reversando pago | URL:{pago.get("idBitacoraPago")}, Request: {inputReversarPago}')

            statusReverso = esperarReversoPago(pago.get('idBitacoraPago'))

            if statusReverso == 'R':
                statusCancelacion.append(True)
            elif statusReverso == 'S' or statusReverso == 'F' or statusReverso == 'E':
                statusCancelacion.append(False)
                break

        except Exception as e:
            print(f'Ha ocurrido un error al cancelar pagos: {e.with_traceback()}')

    return statusCancelacion


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

    print(f'Se ha resuelto espera de pago con status: {statusPago}')
    return statusPago


def obtenerTotalPagos(pagosEnFecha):

    totalPagos = 0.0
    for pago in pagosEnFecha:
        totalPagos += pago.get('monto')

    return round(totalPagos, 2)


def obtenerTipoOperacionLedgerQuebranto(tipoQuebranto):
    codigos = {
        'QUEBRANTO_FALLECIMIENTO': 'QUEBRANTO_FUNERARIO',
        'QUEBRANTO_INVALIDEZ': 'QUEBRANTO_INVALIDEZ_PERMANENTE',
        'QUEBRANTO_PRIVACION_LIBERTAD': 'QUEBRANTO_PRIVACION_LEGAL',
        'QUEBRANTO_FRAUDE': 'QUEBRANTO_FRAUDE',
        'QUEBRANTO_DIAS_ATRASO': 'QUEBRANTO_DIAS_ATRASO',
        'QUEBRANTO_ANTICIPADOS_RECUPERABLES': 'QUEBRANTO_ANTICIPADOS_RECUPERABLES'
    }

    tipoOperacion = codigos.get(tipoQuebranto, None)
    print(f'Input: tipoQuebranto={tipoQuebranto}, tipoOperacion={tipoOperacion}')
    return tipoOperacion


def obtenerCodigoQuebratoPago(tipoQuebranto):
    codigos = {
        'QUEBRANTO_FALLECIMIENTO': 'QFA',
        'QUEBRANTO_INVALIDEZ': 'QIN',
        'QUEBRANTO_PRIVACION_LIBERTAD': 'QPR',
        'QUEBRANTO_FRAUDE': 'QFR'
    }

    codigoMovimiento = codigos.get(tipoQuebranto, None)
    print(f'Input: tipoQuebranto={tipoQuebranto}, codigo={codigoMovimiento}')
    return codigoMovimiento


def obtenerCodigoQuebranto(tipoQuebranto, rubro):
    codigos = {
        'QUEBRANTO_INVALIDEZ': {
            'GASTO_COBRANZA': 'QING',
            'INTERESES': 'QINI',
            'CAPITAL': 'QINC'
        },
        'QUEBRANTO_FALLECIMIENTO': {
            'GASTO_COBRANZA': 'QFAG',
            'INTERESES': 'QFAI',
            'CAPITAL': 'QFAC'
        },
        'QUEBRANTO_DESEMPLEO_INVALIDEZ': {
            'GASTO_COBRANZA': 'QING',
            'INTERESES': 'QINI',
            'CAPITAL': 'QINC'
        },
        'QUEBRANTO_PRIVACION_LIBERTAD': {
            'GASTO_COBRANZA': 'QPLG',
            'INTERESES': 'QPLI',
            'CAPITAL': 'QPLC'
        },
        'QUEBRANTO_FRAUDE': {
            'GASTO_COBRANZA': 'QFRG',
            'INTERESES': 'QFRI',
            'CAPITAL': 'QFRC'
        }
    }

    codigoMovimiento = codigos.get(tipoQuebranto, {}).get(rubro, None)
    print(f"Input: tipoQuebranto={tipoQuebranto}, rubro={rubro} | Output: {codigoMovimiento}")
    return codigoMovimiento


def obtenerCodigoMovimientoPrimaNoDevengada(tipoOperacion, rubro):
    codigos = {
        "SEGURO_FUNERARIO": {
            "GASTO_COBRANZA": "NFUG",
            "INTERESES": "NFUI",
            "CAPITAL": "NFUC",
            "SALDO_FAVOR": "NFUS",
        },
        "SEGURO_DESEMPLEO": {
            "GASTO_COBRANZA": "NDEG",
            "INTERESES": "NDEI",
            "CAPITAL": "NDEC",
            "SALDO_FAVOR": "NDES",
        },
        "SEGURO_PROTECCION_INTEGRAL": {
            "GASTO_COBRANZA": "NPIG",
            "INTERESES": "NPII",
            "CAPITAL": "NPIC",
            "SALDO_FAVOR": "NPIS",
        },
        "SEGURO_VIDA_FINANCIADO": {
            "GASTO_COBRANZA": "NVIG",
            "INTERESES": "NVII",
            "CAPITAL": "NVIC",
            "SALDO_FAVOR": "NVIS",
        },
        "SEGURO_DIABETES": {
            "GASTO_COBRANZA": "NDIG",
            "INTERESES": "NDII",
            "CAPITAL": "NDIC",
            "SALDO_FAVOR": "NDIS",
        },
        "SEGURO_PROTECCION_CELULAR": {
            "GASTO_COBRANZA": "NFUG",
            "INTERESES": "NFUI",
            "CAPITAL": "NFUC",
            "SALDO_FAVOR": "NFUS",
        },
    }

    codigoMovimiento = codigos.get(tipoOperacion, {}).get(rubro, None)
    print(f"Input: tipoOperacion={tipoOperacion}, rubro={rubro} | Output: {codigoMovimiento}")
    return codigoMovimiento


with DAG(
        dag_id='NC-13-Pago',
        default_args=default_args,
        schedule_interval=None
) as dag:

    pago_task = PythonOperator(
        task_id='pago_task',
        provide_context=True,
        python_callable=pago_function
    )

    trigger_task: TriggerDagRunOperator = TriggerDagRunOperator(
        task_id='saga_call_task',
        trigger_dag_id='NC-16-Saga-pago',
        conf={
            'idBitacoraSeguros': "{{ti.xcom_pull(task_ids='pago_task', key='idBitacoraSeguros')}}",
            'idBitacora': "{{ti.xcom_pull(task_ids='pago_task', key='idBitacora')}}",
            'idDisposicion': "{{ti.xcom_pull(task_ids='pago_task', key='idDisposicion')}}",
            'idLineaCredito': "{{ti.xcom_pull(task_ids='pago_task', key='lineaCredito')}}",
            'referencia': "{{ti.xcom_pull(task_ids='pago_task', key='referencia')}}",
            'idTransaccion': "{{ti.xcom_pull(task_ids='pago_task', key='idTransaccion')}}",
            'fechaValor': "{{ti.xcom_pull(task_ids='pago_task', key='fechaValor')}}",
            'pagoHasError': "{{ti.xcom_pull(task_ids='pago_task', key='hasError')}}",
            'fase': "{{ ti.xcom_pull(task_ids='pago_task', key='fase') }}",
            'errMsg': "{{ti.xcom_pull(task_ids='pago_task', key='errMsg')}}",
            'errorResponse': "{{ti.xcom_pull(task_ids='pago_task', key='errorResponse')}}"
        }
    )

pago_task >> [trigger_task]
