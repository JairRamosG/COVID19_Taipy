from taipy.gui import Gui, State
import taipy.gui.builder as tgb
import plotly.express as px
import pandas as pd
import taipy as tp 

def inicio_variables(state: State):
    '''
    Una única inicialización para los valores iniciales que trae el filtro
    '''
    state.edad_min = 0,
    state.edad_max = 100,
    state.sexo_sel = "Todos"
    state.comorb_sel = []


##### FUncion para callback
def cambio_en_filtro(state:State):
    '''
    Se va a ejecutar cuando el usuario haga una modificación en los filtros,
    actualiza los filtros definidos en la configuracion de los Data Nodes
    '''
    # aCTUalizar los filtros
    state.scenario.filtros.write({
        "edad_min" : state.edad_min, 
        "edad_max" : state.edad_max, 
        "sexo" : state.sexo_sel,
        "comorbilidades" : state.comorb_sel # selectores de UI
    })

    # ejecutar el Pipeline Spark
    tp.sumit(state.scenario)

    # Leer resultados procesados por Spark con "id" del archivo de configuracion de nodos entrada/salida
    state.metricas = state.scenario.metricas.read()
    state.datos_graficas = state.scenario.datos_graficas.read()


def build_page():
    '''
    COnstrucción de toda la parte visual de la página
    '''
    with tgb.Page() as page:
        tgb.text("Dashboard Covid 19 México", mode = "md")
        tgb.html("hr")

    return page

##### Exportar página
home_page = build_page()