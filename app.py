from taipy.gui import Gui
from src.pages.home import home_page, inicio_variables, cambio_en_filtro
from src.config.pipeline_config import escenario_config
import taipy as tp
from taipy import Orchestrator


if __name__ == "__main__":
    print("1. Inicializar Orquesatador")
    Orchestrator().run()

    print("2. Creando el escenario")
    escenario = tp.create_scenario(escenario_config)
    print(f"Escenario creado {escenario.id}")

    print("3. Ejecutar el pipeline inicial")    
    try:
        tp.submit(escenario)
        print('Ejecutado correctamente')
    except Exception as e:
        print(f'Error: {e}')

    print("4. Verificar que los DataNodes funcionan")
    try:
        metricas_iniciales = escenario.metricas.read() # DataNode id
        print(f"Metricas iniciales: {metricas_iniciales}")
    except Exception as e:
        print(f"{e}")
        metricas_iniciales = None

    try:
        datos_iniciales = escenario.datos_graficas.read()
        print(f"Datos gráficos: {len(datos_iniciales) if datos_iniciales is not None else 0} registros")
    except Exception as e:
        print(f"Error: {e}")
        datos_iniciales = None
    
    print("5. COnfiigurar la GUI")
    gui = Gui(page = home_page)

    gui.edad_min = 0
    gui.edad_max = 100
    gui.sexo_sel = "Todos"
    gui.comorb_sel = []
    gui.metricas = metricas_iniciales or {
        'Total': 0, 'media_Edad': 0, 'Supervivientes': 0,
        'pct_supervivencia': 0, 'promedio_comorb': 0        
    }
    gui.datos_graficas = datos_iniciales
    gui.scenario = escenario

    def on_init(state):
        pass
        
    gui.on_init = on_init    

    print("6. Ejecutar la aplicación")
    gui.run(
        on_init = on_init,
        use_reloader = True, 
        debug = True,
        title = "Dashboard Covid 19"
    )