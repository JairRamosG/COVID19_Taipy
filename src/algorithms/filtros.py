from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pandas as pd


### INICIALIZAR SPARK UNA SOLA VEZ
_spark = None

def get_Spark_Session():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
        .appName('Covid_Dashboard') \
        .config('spark.sql.adaptative.enabled', 'true') \
        .config('spark.sql.adaptative.coalescePartitions.enabled', 'true') \
        .config('spark.driver.memory', '4g') \
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
        .getOrCreate()
    return _spark


### CARGAR LOS DATOS CON SPARK
def cargar_datos_Spark(ruta_parquet):
    '''
    Carga los datos usando Pyspark directamente desde Parquet
    Args:
        ruta_parquet: ruta de la carpeta de archivos
    Returns:
        Datafrmae de Spark
    '''
    spark = get_Spark_Session()
    df_spark = spark.read.parquet(ruta_parquet)
    df_spark.cache()
    df_spark.count()
    return df_spark

def aplicar_filtros(df_spark, filtros):
    '''
    FIltra el Dataframe de Spark según los parámetros
    Args:
        df_spark : Dataframe de spark
        filtros : diccionario con los filtros
    Return:
        Dataframe de spark ya filtrado
    '''
    df_filtrado = df_spark

    ### FIltro por edad, va a ser considerado como que si existe siempre
    df_filtrado = df_filtrado.filter(
        (F.col('EDAD') >= filtros['edad_min']) &
        (F.col('EDAD') <= filtros['edad_max'])
    )

    ### FIltro por sexo
    if filtros['Sexo'] != 'Todos':
            sexo_map = {1:'Femenino', 2:'Masculino'}
            df_filtrado = df_filtrado.filter(F.col('SEXO') == sexo_map[filtros['Sexo']])
    
    ### FIltro por comorbilidades
    if filtros['comorbilidades']:
         for comorb in filtros['comorbilidades']:
              df_filtrado = df_filtrado.filter(F.col(comorb) == 1)
    
    return df_filtrado


def frecuencia_comorbilidades(df_spark, lista_comorbilidades):
    '''
    Cuenta la frecuencia de comorbilidades del Dataframe que se filtro

    Args:  
        df_spark : Dataframe de Spark con los filtros realizados
        lista_comorbilidades : Lista con las comorbilidades anotadas
    Returns:
        diccionario con la frecuencia de las comorbilidades

    '''

    resultados = []
    for comorb in lista_comorbilidades:
        if comorb in df_spark.columns:
            conteo = df_spark.filter(F.col(comorb)==1).count()
            resultados.append({
                'comorbilidad' : comorb,
                'conteo' : conteo
            })
    return pd.DataFrame(resultados)

def calcula_metricas_principales(df_spark):
    '''
    Caluclar las metricas principales para mostrarlas en un header
    Return:
        DIccionario con los valores de las metricas principales 
    '''
    metricas = df_spark.agg(
        F.count('*').alias('Total'),
        F.avg('EDAD').alias('media_Edad'),
        F.sum(F.when(F.col('Sobrevivio') == 1, 1).otherwise(0)).alias('Sobrevivieron'),
        F.sum(F.when(F.col('Sobrevivio') == 0, 1).otherwise(0)).alias('No_Sobrevivieron'),
        F.avg('N_COMORBILIDADES').alias('promedio_comorb')
    ).collect[0]

    resultado = {
        'Total' : metricas['Total'],
        'media_Edad' : metricas['media_Edad'],
        'Supervivientes' : metricas['Sobrevivieron'],
        'No_Supervivientes' : metricas['No_Sobrevivieron'],
        'promedio_comorb' : metricas['promedio_comorb']
    }

    if resultado['Total'] > 0 :
         resultado['pct_supervivencia'] = round(resultado['Supervivientes'] / resultado['Total'] * 100, 1)
         resultado['pct_mortalidad'] = round(resultado['No_Supervivientes'] / resultado['Total'] * 100, 1) 
         
    return resultado

def datos_graficos(df_spark, limite = 1000):
    '''
    Muestreo para gŕaficos
    Returns:
        pandas Dataframe para usarlo con plotly
    '''
    muestra_spark = df_spark.select('EDAD', 'SOBREVIVIO').sample(0.1).limit(limite)
    muestra_pandas = muestra_spark.toPandas()

    muestra_pandas['Resultado'] = muestra_pandas['SOBREVIVIO'].map({
         1 : 'SOBREVIVIO',
         0 : 'Falleció'
    })

    return muestra_pandas