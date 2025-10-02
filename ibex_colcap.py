import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, types, text
from sqlalchemy.exc import SQLAlchemyError
import sys
import time
import numpy as np
import mysql.connector
from mysql.connector import Error

# Configuración de pandas
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Lista de acciones por mercado
mercados = {
    "IBEX_35": [
        "ANA.MC", "ACX.MC", "ACS.MC", "AENA.MC", "AMS.MC", "MTS.MC", "BBVA.MC", 
        "BKT.MC", "CABK.MC", "CLNX.MC", "CIE.MC", "ENG.MC", "ELE.MC", "FER.MC", 
        "FDR.MC", "GRF.MC", "IAG.MC", "IBE.MC", "ITX.MC", "IDR.MC", "LOG.MC", 
        "MAP.MC", "MEL.MC", "MRL.MC", "NTGY.MC", "PHM.MC", "RED.MC", "REP.MC", 
        "ROVI.MC", "SAB.MC", "SAN.MC", "SLR.MC", "TEF.MC", "UNI.MC", "VIS.MC"
    ],
    "COLCAP": [
        "NUTRESA.CL", "GRUBOLIVAR.CL", "PEI.CL", "CIBEST.CL", "GXTESCOL.CL", 
        "PFCIBEST.CL", "GRUPOSURA.CL", "PFGRUPSURA.CL", "BOGOTA.CL", 
        "PFDAVVNDA.CL", "ISA.CL", "HCOLSEL.CL", "ICOLCAP.CL", "CORFICOLCF.CL", 
        "GRUPOARGOS.CL", "PFCORFICOL.CL", "TERPEL.CL", "BVC.CL", "PFCEMARGOS.CL", 
        "PFGRUPOARG.CL", "CEMARGOS.CL", "MINEROS.CL", "PROMIGAS.CL", "CNEC.CL", 
        "CELSIA.CL", "EXITO.CL", "GEB.CL", "ECOPETROL.CL", "GRUPOAVAL.CL", 
        "PFAVAL.CL", "CONCONCRET.CL", "BHI.CL", "ENKA.CL"
    ]
}

# Crear lista única de todos los tickers con su mercado
lista_acciones = []
for mercado, tickers in mercados.items():
    for ticker in tickers:
        lista_acciones.append({"ticker": ticker, "mercado": mercado})

# Configuración de la conexión a MySQL (XAMPP)
DB_USER = "root"
DB_PASSWORD = ""
DB_HOST = "127.0.0.1"
DB_NAME = "colcap_ibex"  # Cambiado a nombre más genérico
DB_PORT = '3306'

# =============================================================================
# FUNCIONES DE CONEXIÓN MEJORADAS
# =============================================================================

def verificar_mysql():
    """Verifica si MySQL está corriendo antes de intentar conectar"""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        if connection.is_connected():
            print("✅ MySQL está corriendo correctamente")
            connection.close()
            return True
    except Error as e:
        print(f"❌ MySQL NO está corriendo: {e}")
        print("⚠️  Por favor inicia MySQL desde XAMPP primero")
        return False

def crear_engine_con_reintentos():
    """Crea el engine con manejo de reintentos"""
    intentos = 3
    for i in range(intentos):
        try:
            engine = create_engine(
                f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}',
                pool_recycle=3600,  # Recicla conexiones cada hora
                pool_pre_ping=True  # Verifica conexión antes de usar
            )
            # Testear la conexión
            with engine.connect() as test_conn:
                test_conn.execute(text("SELECT 1"))
            print("✅ Engine creado y conexión verificada")
            return engine
        except SQLAlchemyError as e:
            print(f"❌ Intento {i+1} fallido: {e}")
            if i < intentos - 1:
                time.sleep(2)
    return None

# =============================================================================
# FUNCIONES PARA EVITAR DUPLICADOS
# =============================================================================

def obtener_fechas_existentes(engine, tabla, columna_fecha, ticker_columna='ticker'):
    """Obtiene las fechas ya existentes en la base de datos para evitar duplicados"""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT DISTINCT {ticker_columna}, {columna_fecha} FROM {tabla}")
            result = conn.execute(query)
            existing_data = {(row[0], row[1]) for row in result}
        return existing_data
    except Exception as e:
        print(f"Error obteniendo datos existentes de {tabla}: {e}")
        return set()

def filtrar_datos_nuevos(df, existing_data, fecha_columna, ticker_columna='ticker'):
    """Filtra solo los datos que no existen en la base de datos"""
    if df.empty:
        return df
    
    # Crear conjunto de claves únicas (ticker + fecha)
    df_keys = set(zip(df[ticker_columna], df[fecha_columna]))
    
    # Encontrar claves que no existen en la base de datos
    nuevas_claves = df_keys - existing_data
    
    # Filtrar el DataFrame
    mask = df.apply(lambda row: (row[ticker_columna], row[fecha_columna]) in nuevas_claves, axis=1)
    return df[mask]

def guardar_dataframe_seguro(engine, df, nombre_tabla, fecha_columna=None, ticker_columna='ticker'):
    """Guarda un DataFrame de forma segura con verificación de duplicados"""
    if df.empty:
        print(f"⚠️  DataFrame vacío para {nombre_tabla}, omitiendo")
        return False
    
    try:
        # Obtener datos existentes si es una tabla con fechas
        if fecha_columna and fecha_columna in df.columns:
            existing_data = obtener_fechas_existentes(engine, nombre_tabla, fecha_columna, ticker_columna)
            df_filtrado = filtrar_datos_nuevos(df, existing_data, fecha_columna, ticker_columna)
            
            if df_filtrado.empty:
                print(f"⏭️  No hay datos nuevos para {nombre_tabla}, omitiendo")
                return True
                
            print(f"📊 {nombre_tabla}: {len(df_filtrado)} registros nuevos de {len(df)} totales")
            df = df_filtrado
        
        # Usar with para manejo automático de conexión
        with engine.begin() as connection:
            df.to_sql(
                nombre_tabla, 
                connection,
                if_exists='append', 
                index=False,
                method='multi',
                chunksize=1000
            )
        print(f"✓ Tabla '{nombre_tabla}' actualizada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error guardando {nombre_tabla}: {e}")
        return False

def verificar_duplicados(engine):
    """Verifica si hay duplicados en las tablas con fechas"""
    tablas_con_fechas = ['mercado_diario', 'dividendos', 'splits', 'recomendaciones']
    
    for tabla in tablas_con_fechas:
        try:
            with engine.connect() as conn:
                query = text(f"""
                    SELECT ticker, fecha, COUNT(*) as duplicados 
                    FROM {tabla} 
                    GROUP BY ticker, fecha 
                    HAVING COUNT(*) > 1
                """)
                result = conn.execute(query)
                duplicados = result.fetchall()
                
                if duplicados:
                    print(f"⚠️  Duplicados encontrados en {tabla}: {len(duplicados)}")
                else:
                    print(f"✅ Sin duplicados en {tabla}")
                    
        except Exception as e:
            print(f"Error verificando duplicados en {tabla}: {e}")

# =============================================================================
# VERIFICACIÓN INICIAL DE CONEXIÓN
# =============================================================================

print("🔍 Verificando estado de MySQL...")
if not verificar_mysql():
    print("❌ No se puede continuar. Inicia MySQL desde XAMPP y vuelve a ejecutar.")
    sys.exit(1)

print("🔌 Creando engine de conexión...")
engine = crear_engine_con_reintentos()
if not engine:
    print("❌ No se pudo establecer conexión con MySQL")
    sys.exit(1)

# Verificar si la base de datos existe
try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DATABASE()"))
        current_db = result.scalar()
        print(f"Base de datos actual: {current_db}")
        
        if current_db == DB_NAME:
            print(f"✅ Conectado a la base de datos correcta: {DB_NAME}")
        else:
            print(f"⚠️  Conectado a: {current_db}, pero esperábamos: {DB_NAME}")
            print("Creando la base de datos...")
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
            conn.execute(text(f"USE {DB_NAME}"))
            
except Exception as e:
    print(f"❌ Error de conexión: {e}")
    sys.exit(1)

# =============================================================================
# EXTRACCIÓN DE DATOS
# =============================================================================

def obtener_datos_accion(ticker):
    """Función para obtener todos los datos de una acción con mejor manejo de errores"""
    try:
        ticker_obj = yf.Ticker(ticker)
        info = ticker_obj.get_info()
        
        # Pequeña pausa para no saturar la API
        time.sleep(0.5)
        
        # Obtener histórico con manejo de errores
        try:
            historial = ticker_obj.history(period="10y")
            if historial.empty:
                print(f"Advertencia: Sin datos históricos para {ticker}")
        except Exception as e:
            print(f"Error obteniendo histórico para {ticker}: {e}")
            historial = pd.DataFrame()
        
        # Obtener dividendos con manejo de errores
        try:
            dividendos = ticker_obj.dividends
            if dividendos.empty:
                dividendos = pd.Series(dtype=float)
        except Exception as e:
            print(f"Error obteniendo dividendos para {ticker}: {e}")
            dividendos = pd.Series(dtype=float)
        
        # Obtener splits con manejo de errores
        try:
            splits = ticker_obj.splits
            if splits.empty:
                splits = pd.Series(dtype=float)
        except Exception as e:
            print(f"Error obteniendo splits para {ticker}: {e}")
            splits = pd.Series(dtype=float)
        
        # Obtener recomendaciones con manejo de errores
        try:
            recomendaciones = ticker_obj.recommendations
            if recomendaciones is None or recomendaciones.empty:
                recomendaciones = pd.DataFrame()
        except Exception as e:
            print(f"Error obteniendo recomendaciones para {ticker}: {e}")
            recomendaciones = pd.DataFrame()
        
        return {
            "ticker": ticker,
            "info": info,
            "historial": historial,
            "dividendos": dividendos,
            "splits": splits,
            "recomendaciones": recomendaciones
        }
    except Exception as e:
        print(f"Error crítico obteniendo datos para {ticker}: {e}")
        return None

# Función para manejar valores nulos de forma más robusta
def safe_get(data, key, default=None):
    """Obtiene valores de forma segura de diccionarios anidados"""
    if isinstance(data, dict):
        return data.get(key, default)
    return default

# Paso 1: Extraer todos los datos
datos_acciones = {}
for accion in lista_acciones:
    ticker = accion["ticker"]
    mercado = accion["mercado"]
    print(f"Extrayendo datos para {ticker} ({mercado})...")
    datos = obtener_datos_accion(ticker)
    if datos:
        # Agregar información del mercado a los datos
        datos["mercado"] = mercado
        datos_acciones[ticker] = datos
    else:
        print(f"No se pudieron obtener datos para {ticker}")

print("Extracción completada. Creando DataFrames...")

# Paso 2: Crear DataFrames optimizados para SQL con mejor manejo de nulos

# TABLA 1: activos
activos_data = []
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    if ticker in datos_acciones:
        datos = datos_acciones[ticker]
        info = datos["info"]
        
        activos_data.append({
            "ticker": ticker,
            "market": mercado,  # Nueva columna para identificar el mercado
            "name": safe_get(info, "longName", "No disponible"),
            "short name": safe_get(info, "shortName", "No disponible"),
            "business summary": safe_get(info, "longBusinessSummary", "No disponible"),
            "website": safe_get(info, "website", "No disponible"),
            "Phone": safe_get(info, "phone", "No disponible"),
            "address": safe_get(info, "address1", "No disponible"),
            "city": safe_get(info, "city", "No disponible"),
            "state": safe_get(info, "state", "No disponible"),
            "pc": safe_get(info, "zip", "No disponible"),
            "country": safe_get(info, "country", "No disponible"),
            "industry": safe_get(info, "industry", "No disponible"),
            "sector": safe_get(info, "sector", "No disponible"),
            "quote type": safe_get(info, "quoteType", "No disponible"),
            "currency": safe_get(info, "currency", "No disponible"),
            "language": safe_get(info, "language", "No disponible"),
            "region": safe_get(info, "region", "No disponible")
        })
    else:
        print(f"⚠️  No hay datos para {ticker}, omitiendo de la tabla activos")

activos = pd.DataFrame(activos_data)

# TABLA 2: datos_mercado_diario
mercado_data = []
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    if ticker in datos_acciones:
        datos = datos_acciones[ticker]
        historial = datos["historial"]
        
        if not historial.empty:
            historial = historial.reset_index()
            for _, row in historial.iterrows():
                mercado_data.append({
                    "ticker": ticker,
                    "market": mercado,  # Nueva columna para identificar el mercado
                    "date": row["Date"].date() if pd.notna(row["Date"]) else None,
                    "open price": round(row["Open"], 2) if pd.notna(row["Open"]) else 0.00,
                    "high price": round(row["High"], 2) if pd.notna(row["High"]) else 0.00,
                    "low price": round(row["Low"], 2) if pd.notna(row["Low"]) else 0.00,
                    "closing price": round(row["Close"], 2) if pd.notna(row["Close"]) else 0.00,
                    "volume": int(row["Volume"]) if pd.notna(row["Volume"]) else 0
                })

mercado_diario = pd.DataFrame(mercado_data)

# TABLA 3: rendimiento_financiero
rendimiento_data = []
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    if ticker in datos_acciones:
        datos = datos_acciones[ticker]
        info = datos["info"]
        
        rendimiento_data.append({
            "ticker": ticker,
            "market": mercado,  # Nueva columna para identificar el mercado
            "market cap": safe_get(info, "marketCap", 0),
            "avg price 50 days": safe_get(info, "fiftyDayAverage", 0),
            "avg price 200 days": safe_get(info, "twoHundredDayAverage", 0),
            "change percent 52 weeks": safe_get(info, "fiftyTwoWeekChangePercent", 0)
        })

rendimiento_financiero = pd.DataFrame(rendimiento_data)

# TABLA 4: estados_financieros
estados_data = []
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    if ticker in datos_acciones:
        datos = datos_acciones[ticker]
        info = datos["info"]
        
        estados_data.append({
            "ticker": ticker,
            "market": mercado,  # Nueva columna para identificar el mercado
            "total cash": safe_get(info, "totalCash", 0),
            "total debt": safe_get(info, "totalDebt", 0),
            "total revenue": safe_get(info, "totalRevenue", 0),
            "profit margins": safe_get(info, "profitMargins", 0),
            "gross profits": safe_get(info, "grossProfits", 0),
            "free cash flow": safe_get(info, "freeCashflow", 0),
            "operating cash flow": safe_get(info, "operatingCashflow", 0),
            "revenue growth": safe_get(info, "revenueGrowth", 0),
            "ebitda": safe_get(info, "ebitda", 0),
            "net income to common": safe_get(info, "netIncomeToCommon", 0),
            "financial currency": safe_get(info, "financialCurrency", "No disponible"),
            
            
            
            
            
            "price to sale ratio 12 months": safe_get(info, "priceToSalesTrailing12Months", 0),
            "enterprise to revenue": safe_get(info, "enterpriseToRevenue", 0),
            "enterprise to_ebitda": safe_get(info, "enterpriseToEbitda", 0),
            "price to earnings": safe_get(info, "trailingPE", 0),
            "per futuro": safe_get(info, "forwardPE", 0),
            "price to book": safe_get(info, "priceToBook", 0),
            "debt to equity": safe_get(info, "debtToEquity", 0),
            "roa": safe_get(info, "returnOnAssets", 0),
            "roe": safe_get(info, "returnOnEquity", 0),
            "eps ttm": safe_get(info, "epsTrailingTwelveMonths", 0),
            "eps fordward": safe_get(info, "epsForward", 0)
        })

estados_financieros = pd.DataFrame(estados_data)

# TABLA 5: Dividendos
data_dividendos = {}
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    try:
        ticker_obj = yf.Ticker(ticker)
        dividendos_acciones = ticker_obj.dividends
        data_dividendos[ticker] = dividendos_acciones
    except Exception as e:
        print(f"Error obteniendo dividendos de {ticker}: {e}")

# Crear DataFrame y limpiar
dividendos = pd.DataFrame(data_dividendos)
dividendos.fillna(0, inplace=True)

# CORRECCIÓN: Verificar y convertir el índice apropiadamente
if not dividendos.empty:
    # Asegurar que el índice es DateTimeIndex y luego extraer la fecha
    dividendos.index = pd.to_datetime(dividendos.index)
    dividendos.index = dividendos.index.date  # Esto ahora funcionará
    
    # Resto del procesamiento
    dividendos = dividendos.reset_index().rename(columns={'index': 'Date'})
    dividendos = dividendos.melt(id_vars=['Date'], var_name='ticker', value_name='dividendo')
    dividendos = dividendos[dividendos['dividendo'] != 0]  # Eliminar filas con dividendos cero
    
    # Agregar información del mercado
    mercado_dict = {accion["ticker"]: accion["mercado"] for accion in lista_acciones}
    dividendos['mercado'] = dividendos['ticker'].map(mercado_dict)
else:
    print("No se encontraron datos de dividendos")
    dividendos = pd.DataFrame(columns=['Date', 'ticker', 'dividendo', 'mercado'])
    dividendos = dividendos.rename(columns={'dividendo': 'divident', 'mercado': 'market'})

# TABLA 6: Splits
splits_data = {}
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    ticker_obj = yf.Ticker(ticker)
    splits_acciones = ticker_obj.splits
    splits_data[ticker] = splits_acciones

splits = pd.DataFrame(splits_data)
splits.fillna(0, inplace=True)
splits = splits.reset_index().rename(columns={'index': 'Date'})
splits = splits.melt(id_vars=['Date'], var_name='ticker', value_name='split_ratio')
splits = splits[splits['split_ratio'] != 0]  # Eliminar filas con splits cero

# Agregar información del mercado
mercado_dict = {accion["ticker"]: accion["mercado"] for accion in lista_acciones}
splits['mercado'] = splits['ticker'].map(mercado_dict)

# Renombrar columnas finales
splits = splits.rename(columns={'split_ratio': 'split ratio','mercado': 'market'})

# TABLA 7: Recomendaciones
all_recommendations_list = []
for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    ticker_obj = yf.Ticker(ticker)
    recommendations_df = ticker_obj.recommendations
    
    if recommendations_df is not None and not recommendations_df.empty:
        recommendations_df['ticker'] = ticker
        recommendations_df['mercado'] = mercado  # Nueva columna para identificar el mercado
        recommendations_df = recommendations_df.reset_index()
        all_recommendations_list.append(recommendations_df)

# Concatenar todos los DataFrames de la lista en uno solo
recomendaciones = pd.concat(all_recommendations_list) if all_recommendations_list else pd.DataFrame()

# TABLA 8: Consenso analistas  
nombres_empresas = {}

for accion_info in lista_acciones:
    ticker = accion_info["ticker"]
    mercado = accion_info["mercado"]
    
    ticker_obj = yf.Ticker(ticker)
    info = ticker_obj.get_info()  
    
    calificacion_media = info.get("recommendationMean", 0)
    num_analistas = info.get("numberOfAnalystOpinions", 0)
    precio_objetivo = info.get("targetMeanPrice", 0)
    
    nombres_empresas[ticker] = {
        "mercado": mercado,  # Nueva columna para identificar el mercado
        "calificacion_media_recomendacion": round(calificacion_media, 2),
        "No_analistas": int(num_analistas),
        "precio_objetivo_medio_COP": int(precio_objetivo)
    }

# Crea el DataFrame a partir del diccionario, con los tickers como índice
consenso_analistas = pd.DataFrame.from_dict(nombres_empresas, orient='index')
consenso_analistas = consenso_analistas.reset_index().rename(columns={'index': 'ticker',
            'mercado': 'market', 'calificacion_media_recomendacion': 'average analyst recommendation rating',
            'No_analistas': 'number of analysts','precio_objetivo_medio_COP': 'average price'})
                                                                   

# Mostrar información de los DataFrames creados
print("\nResumen de datos extraídos:")
print(f"Activos: {activos.shape}")
print(f"Datos mercado diario: {mercado_diario.shape}")
print(f"Rendimiento financiero: {rendimiento_financiero.shape}")
print(f"Estados financieros: {estados_financieros.shape}")
print(f"Dividendos: {dividendos.shape}")
print(f"Splits: {splits.shape}")
print(f"Recomendaciones: {recomendaciones.shape if not recomendaciones.empty else '0 registros'}")
print(f"Consenso analistas: {consenso_analistas.shape}")

# =============================================================================
# GUARDADO EN MYSQL CON MANEJO SEGURO Y SIN DUPLICADOS
# =============================================================================

try:
    print("💾 Iniciando guardado seguro en MySQL...")
    
    # Diccionario de tablas a guardar con sus columnas de fecha
    tablas_config = {
        'activos': {'df': activos, 'fecha_columna': None},
        'mercado_diario': {'df': mercado_diario, 'fecha_columna': 'fecha'},
        'rendimiento_financiero': {'df': rendimiento_financiero, 'fecha_columna': None},
        'estados_financieros': {'df': estados_financieros, 'fecha_columna': None},
        'dividendos': {'df': dividendos, 'fecha_columna': 'Date'},
        'splits': {'df': splits, 'fecha_columna': 'Date'},
        'recomendaciones': {'df': recomendaciones, 'fecha_columna': 'Date'},
        'consenso_analistas': {'df': consenso_analistas, 'fecha_columna': None}
    }
    
    resultados = {}
    for nombre_tabla, config in tablas_config.items():
        resultados[nombre_tabla] = guardar_dataframe_seguro(
            engine, 
            config['df'], 
            nombre_tabla, 
            config['fecha_columna']
        )
    
    # Resumen de resultados
    print("\n📊 Resumen de guardado:")
    exitos = sum(resultados.values())
    total = len(resultados)
    print(f"✅ {exitos}/{total} tablas actualizadas exitosamente")
    
    for tabla, exito in resultados.items():
        status = "✅" if exito else "❌"
        print(f"{status} {tabla}")

except Exception as e:
    print(f"❌ Error durante el proceso de guardado: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Verificar duplicados
    print("\n🔍 Verificando duplicados...")
    verificar_duplicados(engine)
    
    # CIERRE DE CONEXIONES
    print("\n🔌 Cerrando conexiones...")
    try:
        engine.dispose()
        print("✅ Engine y conexiones cerrados correctamente")
    except Exception as e:
        print(f"⚠️  Error cerrando engine: {e}")

# =============================================================================
# VERIFICACIÓN FINAL
# =============================================================================

def verificar_tablas_creadas():
    """Verifica las tablas creadas usando una conexión temporal"""
    try:
        temp_engine = create_engine(
            f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
        with temp_engine.connect() as conn:
            result = conn.execute(text("SHOW TABLES"))
            tablas = [row[0] for row in result]
            
            print("\n📊 Tablas en la base de datos:")
            for tabla in tablas:
                count_result = conn.execute(text(f"SELECT COUNT(*) FROM {tabla}"))
                count = count_result.scalar()
                print(f"• {tabla}: {count} registros")
                
            # Verificar distribución por mercado
            if 'activos' in tablas:
                result = conn.execute(text("SELECT mercado, COUNT(*) FROM activos GROUP BY mercado"))
                print("\n📈 Distribución por mercado:")
                for row in result:
                    print(f"• {row[0]}: {row[1]} activos")
        
        temp_engine.dispose()
        
    except Exception as e:
        print(f"Error verificando tablas: {e}")

# Ejecutar verificación
verificar_tablas_creadas()

print("\n🎉 Proceso completado! Los datos nuevos se han agregado sin duplicar los existentes.")
