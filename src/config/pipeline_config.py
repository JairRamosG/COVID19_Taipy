from taipy import Config
from algorithms import filtros

##################### Definicion de los Data Nodes de ENTRADA

# para los datos
covid_data_config = Config.configure_data_node(
    id = "covid_data",
    storage_type = "parquet",
    path = "data/parquet/df_final.parquet",
    scope = "global"
    )
# para lso filtros
filter_config = Config.configure_data_node(
    id = "filtros",
    storage_type = "pickle",
    default_data = {
        "edad_min" : 0,
        "edad_max" : 100,
        "Sexo" : "Todos",
        "comorbilidades" : []
    }
)

### Definicion de los Data Nodes de SALIDA

# para los resultados
resultado_config = Config.configure_data_node(
    id = "resultado",
    storage_type = "pickle"
)

# para las metricas
metricas_config = Config.configure_data_node(
    id = "metricas",
    storage_type = "pickle"
)

# para la info de las gráficas
datos_graficas_config = Config.configure_data_node(
    id = "datos_graficas",
    storage_type = "pickle"
)


##################### Definicion de las TASK

# para aplciar filtros
filtrar_task_config = Config.configure_task(
    id = "filtrar_datos",
    function = filtros.aplicar_filtros,
    inputs = [covid_data_config, filter_config],
    outputs = [resultado_config]
)

# para calcular las metricas
calcula_metricas_task_config = Config.configure_task(
    id = 'calcula_metricas',
    function = filtros.calcula_metricas_principales,
    inputs = [resultado_config],
    outputs = [metricas_config]
)

# para los datos de los gráficos
datos_graficas_task_config = Config.configure_task(
    id = 'datos_graficas',
    function = filtros.datos_graficos,
    inputs = [resultado_config],
    outputs = [datos_graficas_config]
)

##################### Definir el PIPELINE

main_pipeline_config = Config.configure_pipeline(
    id = "main_pipeline",
    task_configs = [
        filtrar_task_config,
        calcula_metricas_task_config,
        datos_graficas_task_config
    ]
)

##################### Definir un ESCENARIO

escenario_config = Config.configure_scenario(
    id = "main_escenario",
    pipeline_configs = [main_pipeline_config]
)

##################### Exportar las CONFIGURACIONES

__all__ = [
    covid_data_config,
    filter_config,
    resultado_config,
    metricas_config,
    datos_graficas_config,
    main_pipeline_config,
    escenario_config
]