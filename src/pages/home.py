from taipy.gui import Gui, State
import taipy.gui.builder as tgb
import plotly.express as px
import pandas as pd
import taipy as tp 

def inicio_variables(state: State):
    '''
    Una única inicialización para los valores iniciales que trae el filtro
    '''
    state.edad_min = 0
    state.edad_max = 100
    state.sexo_sel = "Todos"
    state.comorb_sel = []


########## FUnciones para callback que se aplican en las TASK
def cambio_en_filtro(state:State):
    '''
    Se va a ejecutar cuando el usuario haga una modificación en los filtros,
    actualiza los filtros definidos en la configuracion de los Data Nodes
    '''
    # aCTUalizar TASK filtros con "id" del archivo de configuracion de nodos entrada/salida
    state.scenario.filtros.write({
        "edad_min" : state.edad_min, 
        "edad_max" : state.edad_max, 
        "sexo" : state.sexo_sel,
        "comorbilidades" : state.comorb_sel # selectores de UI
    })

    # ejecutar el Pipeline Spark
    tp.submit(state.scenario)

    # actualizar TASK metricas y datos para graficas
    state.metricas = state.scenario.metricas.read()
    state.datos_graficas = state.scenario.datos_graficas.read()

########## Funciones para las gráficas
def crear_histograma(datos_grafico):
    '''
    Histograma para ver las edades
    '''
    if datos_grafico is None or len(datos_grafico) == 0:
        return px.bar(title = "Sin datos")
    histograma = px.histogram(
        datos_grafico, 
        x = "EDAD",
        color = "Resultado",
        title = "Distribución de edades por resultado",
        color_discrete_map={'Sobrevivio': '#2ecc71', 'Fallecio': '#e74c3c'},
        barmode = "overlay",
        nbins = 50,
        opacity = 0.7       
    )
    return histograma

########## Parte visual de la página
def build_page():
    '''
    COnstrucción de toda la parte visual de la página
    '''
    with tgb.Page() as page:
        tgb.text("Dashboard Covid 19 México", mode = "md")
        tgb.html("hr")
    
        with tgb.layout(columns = "1 1 1 1 1"):
            tgb.indicator(
                value = lambda state: state.metricas.get('Total', 0) if state.metricas else 0,
                label = "Total pacientes"
            )

            tgb.indicator(
                value = lambda state : state.metricas.get('media_Edad', 0) if state.metricas else 0,
                label = "Edad media"
            )

            tgb.indicator(
                value = lambda state : state.metricas.get('Supervivientes', 0) if state.metricas else 0,
                label = "Supervivientes"
            )

            tgb.indicator(
                value = lambda state: state.metricas.get('pct_supervivencia', 0) if state.metricas else 0,
                label = "Porcentaje supervivencia"
            )

            tgb.indicator(
                value = lambda state: state.metricas.get('promedio_comorb', 0) if state.metricas else 0,
                label = "Promedio de comorbilidades"
            )

        tgb.html("hr")


    return page

########## Exportar página
home_page = build_page()