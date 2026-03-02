from taipy.gui import Gui
from src.pages.home import home_page, inicio_variables, cambio_en_filtro
from src.config.pipeline_config import escenario_config
import taipy as tp


if __name__ == "__main__":
    print("1. Inicializar el core Taipy")
    tp.Core().run()

    print("2. Creando el escenario")
    escenario = tp.create_scenario(escenario_config)
    print(f"Escenario creado {escenario.id}")

    print("3. Ejecutar el pipeline con los datos iniciales")
    
    try:
        tp.submit(escenario)
    except Exception as e:
        print(f'{e}')

    print("4. Verificar que los DataNodes funcionan")
    try:
        metricas_iniciales = escenario.metricas.read() # DataNode id
        print(f"Metricas iniciales: {metricas_iniciales}")

        datos_iniciales = escenario.datos_graficas.read() # DataNode id
        print(f"Datos gráficos: {len(datos_iniciales) if datos_iniciales is not None else 0} registros")
    except Exception as e:
        print(f"{e}")
    
    print("5. COnfiigurar la GUI")
    gui = Gui(page = home_page)

    # Variables de estado
    gui._set_state(inicio_variables)

    # Pasar el escenario al estado de la GUI
    gui.state.scenario = escenario
 
    print("6. Ejecutar la aplicación")
    gui.run(
        use_reloader = True, 
        debug = True,
        title = "Dashboard Covid 19"
    )