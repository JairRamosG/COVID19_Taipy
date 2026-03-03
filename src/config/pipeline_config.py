from taipy import Config
from src.algorithms import filtros
from taipy.common.config import Scope

##################### Definicion de los Data Nodes de ENTRADA

# para los datos
ruta_config = Config.configure_data_node(
    id = "ruta_datos",
    storage_type = "pickle",
    default_data = "data/parquet/df_final/", 
    scope = Scope.SCENARIO
)

# para lso filtros
filter_config = Config.configure_data_node(
    id = "filtros",
    storage_type = "pickle",
    default_data = {
        "edad_min" : 0,
        "edad_max" : 100,
        "sexo" : "Todos",
        "comorbilidades" : []
    },
    scope = Scope.SCENARIO
)

### Definicion de los Data Nodes de SALIDA

# para los resultados
resultado_config = Config.configure_data_node(
    id = "resultado",
    storage_type = "pickle",
    scope = Scope.SCENARIO
)

# para las metricas
metricas_config = Config.configure_data_node(
    id = "metricas",
    storage_type = "pickle",
    scope = Scope.SCENARIO
)

# para la info de las gráficas
datos_graficas_config = Config.configure_data_node(
    id = "datos_graficas",
    storage_type = "pickle",
    scope = Scope.SCENARIO
)


##################### Definicion de las TASK

# para aplciar filtros
filtrar_task_config = Config.configure_task(
    id = "task_filtrar_datos",
    function = filtros.aplicar_filtros,
    input = [ruta_config, filter_config],
    output = resultado_config
)

# para calcular las metricas
calcula_metricas_task_config = Config.configure_task(
    id = 'task_calcula_metricas',
    function = filtros.calcula_metricas_principales,
    input = [resultado_config],
    output = metricas_config
)

# para los datos de los gráficos
datos_graficas_task_config = Config.configure_task(
    id = 'task_datos_graficas_',
    function = filtros.datos_graficos,
    input = [resultado_config],
    output = datos_graficas_config
)

##################### Definir un ESCENARIO sin pipeline intermedio

escenario_config = Config.configure_scenario(
    id = "main_escenario",
    task_configs = [
        filtrar_task_config,
        calcula_metricas_task_config,
        datos_graficas_task_config
    ]
)

##################### Exportar las CONFIGURACIONES

__all__ = [
    covid_data_config,
    filter_config,
    resultado_config,
    metricas_config,
    datos_graficas_config,
    escenario_config
]