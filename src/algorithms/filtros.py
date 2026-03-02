# src/algorithms/filtros.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd


def get_spark_session():
    return SparkSession.builder \
        .appName('Covid_Dashboard') \
        .master('local[*]') \
        .config('spark.sql.adaptive.enabled', 'true') \
        .config('spark.driver.memory', '8g') \      
        .config('spark.executor.memory', '8g') \      
        .config('spark.sql.shuffle.partitions', '200') \
        .config('spark.driver.maxResultSize', '4g') \
        .getOrCreate()

def aplicar_filtros(covid_data_pd, filtros):
    print("="*50)
    print("1. Iniciando función aplicar_filtros")
    print(f"2. Tipo de covid_data_pd: {type(covid_data_pd)}")
    
    import time
    start_total = time.time()
    
    print("3. Obteniendo sesión Spark...")
    spark = get_spark_session()
    print(f"   → Sesión Spark obtenida en {time.time()-start_total:.1f}s")
    
    print("4. Creando DataFrame Spark...")
    start = time.time()
    covid_data = spark.createDataFrame(covid_data_pd)
    print(f"   → DataFrame Spark creado en {time.time()-start:.1f}s")
    
    print("5. Aplicando filtro de edad...")
    start = time.time()
    df_filtrado = covid_data.filter(
        (F.col('EDAD') >= filtros['edad_min']) &
        (F.col('EDAD') <= filtros['edad_max'])
    )
    print(f"   → Filtro edad aplicado en {time.time()-start:.1f}s")
    
    print("6. Cacheando resultados...")
    start = time.time()
    df_filtrado.cache()
    print(f"   → Cache configurado en {time.time()-start:.1f}s")
    
    print("7. Contando resultados...")
    start = time.time()
    count = df_filtrado.count()
    print(f"   → Conteo completado en {time.time()-start:.1f}s")
    print(f"   → Registros resultantes: {count}")
    
    print(f"✅ Tiempo total: {time.time()-start_total:.1f}s")
    print("="*50)
    
    return df_filtrado

def calcula_metricas_principales(df_spark):
    '''
    Calcular las metricas principales
    Return:
        Diccionario con los valores de las metricas principales 
    '''
    # Inicializar Spark (¡también necesario aquí!)
    spark = get_spark_session()
    
    print("Calculando métricas...")
    
    metricas = df_spark.agg(
        F.count('*').alias('Total'),
        F.avg('EDAD').alias('media_Edad'),
        F.sum(F.when(F.col('SOBREVIVIO') == 1, 1).otherwise(0)).alias('Sobrevivieron'),
        F.sum(F.when(F.col('SOBREVIVIO') == 0, 1).otherwise(0)).alias('No_Sobrevivieron'),
        F.avg('N_COMORBILIDADES').alias('promedio_comorb')
    ).collect()[0]

    resultado = {
        'Total': metricas['Total'],
        'media_Edad': round(metricas['media_Edad'], 1) if metricas['media_Edad'] else 0,
        'Supervivientes': metricas['Sobrevivieron'] or 0,
        'No_Supervivientes': metricas['No_Sobrevivieron'] or 0,
        'promedio_comorb': round(metricas['promedio_comorb'], 1) if metricas['promedio_comorb'] else 0
    }

    if resultado['Total'] > 0:
        resultado['pct_supervivencia'] = round(
            resultado['Supervivientes'] / resultado['Total'] * 100, 1
        )
        resultado['pct_mortalidad'] = round(
            resultado['No_Supervivientes'] / resultado['Total'] * 100, 1
        )
    
    print(f"Métricas calculadas: {resultado}")
    return resultado


def datos_graficos(df_spark, limite=1000):
    '''
    Muestreo para gráficos
    Returns:
        pandas Dataframe para usarlo con plotly
    '''
    # Inicializar Spark
    spark = get_spark_session()
    
    print("Preparando datos para gráficos...")
    
    muestra_spark = df_spark.select('EDAD', 'SOBREVIVIO').sample(0.1).limit(limite)
    muestra_pandas = muestra_spark.toPandas()

    if len(muestra_pandas) > 0:
        muestra_pandas['Resultado'] = muestra_pandas['SOBREVIVIO'].map({
            1: 'Sobrevivio',
            0: 'Fallecio'
        })
    
    print(f"Datos gráficos: {len(muestra_pandas)} registros")
    return muestra_pandas