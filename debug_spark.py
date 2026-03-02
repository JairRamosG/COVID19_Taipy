# debug_spark.py
from pyspark.sql import SparkSession
import pandas as pd

print("1. Leyendo Parquet con pandas...")
df_pd = pd.read_parquet("data/parquet/df_final.parquet")
print(f"   Shape: {df_pd.shape}")
print(f"   Columnas: {list(df_pd.columns)}")

print("\n2. Analizando tipos de cada columna:")
for col in df_pd.columns:
    print(f"\n{col}:")
    print(f"   dtype pandas: {df_pd[col].dtype}")
    print(f"   valores nulos: {df_pd[col].isnull().sum()}")
    print(f"   valores únicos: {df_pd[col].nunique()}")
    if df_pd[col].dtype == 'object':
        print(f"   muestra: {df_pd[col].iloc[0] if len(df_pd) > 0 else 'N/A'}")
        print(f"   tipos en la columna: {set(type(x).__name__ for x in df_pd[col].dropna().iloc[:100])}")

print("\n3. Intentando crear DataFrame Spark...")
spark = SparkSession.builder \
    .appName('Debug') \
    .master('local[*]') \
    .config('spark.driver.memory', '8g') \
    .getOrCreate()

try:
    # Intentar con inferencia automática
    df_spark = spark.createDataFrame(df_pd)
    print("✅ Éxito con inferencia automática!")
except Exception as e:
    print(f"❌ Error: {e}")
    
    # Si falla, intentar con esquema explícito
    print("\n4. Intentando con esquema explícito...")
    from pyspark.sql.types import *
    
    # Crear esquema basado en pandas
    schema = StructType()
    for col in df_pd.columns:
        if df_pd[col].dtype == 'int64':
            schema.add(col, IntegerType(), True)
        elif df_pd[col].dtype == 'float64':
            schema.add(col, FloatType(), True)
        elif df_pd[col].dtype == 'object':
            # Para columnas de texto
            schema.add(col, StringType(), True)
        else:
            print(f"⚠️ Tipo no manejado para {col}: {df_pd[col].dtype}")
    
    try:
        df_spark = spark.createDataFrame(df_pd, schema=schema)
        print("✅ Éxito con esquema explícito!")
    except Exception as e2:
        print(f"❌ Error con esquema explícito: {e2}")

spark.stop()