# src/algorithms/filtros.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import time


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

def aplicar_filtros(ruta_parquet, filtros):
    '''
    Filtra los datos leyendo directamente con Spark desde la ruta
    
    Args:
        ruta_parquet: Ruta a la carpeta con archivos Parquet
        filtros: Diccionario con los filtros a aplicar
    
    Returns:
        DataFrame de Spark filtrado
    '''
    inicio_total = time.time()
    
    print("\n" + "="*60)
    print("APLICANDO FILTROS CON SPARK")
    print("="*60)
    
    # 1. Obtener sesión Spark
    t0 = time.time()
    spark = get_spark_session()
    print(f"Sesión Spark lista: {time.time()-t0:.1f}s")
    
    # 2. Leer Parquet directamente con Spark
    t1 = time.time()
    print(f"Leyendo Parquet desde: {ruta_parquet}")
    
    try:
        # Leer la carpeta completa (todas las particiones)
        covid_data = spark.read.parquet(ruta_parquet)
        total_registros = covid_data.count()
        print(f"   → Particiones: {covid_data.rdd.getNumPartitions()}")
        print(f"   → Registros totales: {total_registros:,}")
        print(f"Lectura completada: {time.time()-t1:.1f}s")
    except Exception as e:
        print(f"Error leyendo Parquet: {e}")
        raise
    
    # 3. Aplicar filtro por edad (siempre presente)
    t2 = time.time()
    print(f"Aplicando filtros: {filtros}")
    
    df_filtrado = covid_data.filter(
        (F.col('EDAD') >= filtros['edad_min']) &
        (F.col('EDAD') <= filtros['edad_max'])
    )
    
    # 4. Filtro por sexo (opcional)
    if filtros.get('sexo', 'Todos') != 'Todos':
        sexo_valor = 1 if filtros['sexo'] == 'Femenino' else 2
        df_filtrado = df_filtrado.filter(F.col('SEXO') == sexo_valor)
        print(f"   → Filtro sexo aplicado: {filtros['sexo']} ({sexo_valor})")
    
    # 5. Filtro por comorbilidades (opcional)
    if filtros.get('comorbilidades'):
        for comorb in filtros['comorbilidades']:
            if comorb in df_filtrado.columns:
                df_filtrado = df_filtrado.filter(F.col(comorb) == 1)
                print(f"   → Filtro {comorb} aplicado")
    
    print(f"Filtros aplicados: {time.time()-t2:.1f}s")
    
    # 6. Cachear resultados para futuras operaciones
    t3 = time.time()
    df_filtrado.cache()
    count = df_filtrado.count()
    print(f"Cacheado: {count:,} registros ({time.time()-t3:.1f}s)")
    
    # 7. Resumen final
    print(f"\nRESUMEN FILTROS:")
    print(f"   → Registros originales: {total_registros:,}")
    print(f"   → Registros después de filtros: {count:,}")
    print(f"   → Reducción: {(1 - count/total_registros)*100:.1f}%")
    print(f"Tiempo total: {time.time()-inicio_total:.1f}s")
    print("="*60 + "\n")
    
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