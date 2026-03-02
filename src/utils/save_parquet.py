from pathlib import Path
import os

def save_data(df, carpeta, particiones = None):
    '''
    FUnción para guardar el dataset final en un archivo parquet
    Args:
        df: DataFrame de PySpark a guardar
        nombre (str): nombre para el nuevo archivo parquet
        particiones; numero de particiones
    Returns:
        ruta donde se guardo
    '''
    BASE_DIR = Path(os.getcwd()).parent
    PARQUET_DIR = BASE_DIR / 'data' / 'parquet' / carpeta 

    PARQUET_DIR.parent.mkdir(parents=True, exist_ok=True)

    if particiones:
        df = df.repartition(particiones)
    else:
        import multiprocessing
        num_cpus = multiprocessing.cpu_count()
        df = df.prepartition(num_cpus * 2)
    
    print(f'Guardando {df.count():,} registros en {PARQUET_DIR}')
    print(f'particiones {df.rdd.getNumPartitions()}')


    # Guardar como muchos parquets
    df.write.mode('overwrite') \
    .option('compression', 'snappy') \
    .parquet(str(PARQUET_DIR))

    if PARQUET_DIR.exists():
        total_size = sum(f.stat().st_size for f in PARQUET_DIR.glob('**/*') if f.is_file())
        print(f'Archivo guardado en: {PARQUET_DIR}')
        print(f'Tamaño total: {total_size / 1e6:.2f} MB')
        print(f'Particiones: {len(list(PARQUET_DIR.glob("*.parquet")))}')
    else:
        print('No se pudo guardar el archivo')
    
    return str(PARQUET_DIR)