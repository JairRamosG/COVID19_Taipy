# src/pages/home.py
from taipy.gui import Gui, State
import taipy.gui.builder as tgb
import plotly.express as px
import pandas as pd
import taipy as tp

# ============================================
# FUNCIONES DE CALLBACK (¡igual que antes!)
# ============================================
def on_filter_change(state: State):
    """
    Se ejecuta cuando el usuario cambia cualquier filtro
    """
    # 1. Actualizar filtros en el Data Node de Taipy
    state.scenario.filtros.write({
        'edad_min': state.edad_min,
        'edad_max': state.edad_max,
        'sexo': state.sexo_sel,
        'comorbilidades': state.comorb_sel
    })
    
    # 2. Ejecutar pipeline Spark
    tp.submit(state.scenario)
    
    # 3. Leer resultados procesados por Spark
    state.metricas = state.scenario.metricas.read()
    state.datos_grafico = state.scenario.datos_grafico.read()


def on_init(state: State):
    """
    Inicialización UNA SOLA VEZ
    """
    state.edad_min = 0
    state.edad_max = 100
    state.sexo_sel = "Todos"
    state.comorb_sel = []
    on_filter_change(state)


# ============================================
# FUNCIONES PARA GRÁFICOS (igual que antes)
# ============================================
def crear_histograma(datos_grafico):
    """Histograma de edades"""
    if datos_grafico is None or len(datos_grafico) == 0:
        return px.bar(title="Sin datos")
    
    return px.histogram(
        datos_grafico,
        x='EDAD',
        color='resultado',
        title="Distribución de edades por resultado",
        color_discrete_map={'Sobrevivió': '#2ecc71', 'Falleció': '#e74c3c'},
        barmode='overlay',
        nbins=50,
        opacity=0.7
    )


def crear_boxplot_edades(datos_grafico):
    """Boxplot de edades"""
    if datos_grafico is None or len(datos_grafico) == 0:
        return px.bar(title="Sin datos")
    
    return px.box(
        datos_grafico,
        x='resultado',
        y='EDAD',
        title="Distribución de edad por resultado",
        color='resultado',
        color_discrete_map={'Sobrevivió': '#2ecc71', 'Falleció': '#e74c3c'}
    )


def crear_grafico_comorbilidades(datos_grafico):
    """Gráfico de comorbilidades"""
    if datos_grafico is None or len(datos_grafico) == 0:
        return px.bar(title="Sin datos")
    
    comorbilidades = ['DIABETES', 'HIPERTENSION', 'OBESIDAD', 'ASMA', 
                      'EPOC', 'INMUSUPR', 'RENAL_CRONICA', 'TABAQUISMO']
    
    datos = []
    for c in comorbilidades:
        if c in datos_grafico.columns:
            conteo = datos_grafico[datos_grafico[c] == 1].shape[0]
            datos.append({'comorbilidad': c, 'conteo': conteo})
    
    df_comorb = pd.DataFrame(datos).sort_values('conteo', ascending=True)
    
    return px.bar(
        df_comorb,
        x='conteo',
        y='comorbilidad',
        title="Frecuencia de comorbilidades",
        orientation='h',
        color='conteo',
        color_continuous_scale='Reds',
        text='conteo'
    )


# ============================================
# CONSTRUCCIÓN DE LA PÁGINA CON PYTHON (tgb)
# ============================================
def build_page():
    """Construye toda la interfaz usando Python puro"""
    
    with tgb.Page() as page:
        # Título
        tgb.text("# 🦠 Dashboard COVID-19 México", mode="md")
        tgb.html("hr")
        
        # ========================================
        # MÉTRICAS (5 columnas)
        # ========================================
        with tgb.layout(columns="1 1 1 1 1"):
            # Usamos expresiones lambda para acceso seguro a diccionarios
            tgb.indicator(
                value=lambda state: state.metricas.get('total', 0) if state.metricas else 0,
                label="Total pacientes"
            )
            tgb.indicator(
                value=lambda state: state.metricas.get('edad_media', 0) if state.metricas else 0,
                label="Edad media",
                format="%.1f"
            )
            tgb.indicator(
                value=lambda state: state.metricas.get('sobrevivieron', 0) if state.metricas else 0,
                label="Sobrevivieron"
            )
            tgb.indicator(
                value=lambda state: state.metricas.get('fallecidos', 0) if state.metricas else 0,
                label="Fallecidos"
            )
            tgb.indicator(
                value=lambda state: state.metricas.get('comorb_prom', 0) if state.metricas else 0,
                label="Comorb. promedio",
                format="%.1f"
            )
        
        tgb.html("hr")
        
        # ========================================
        # FILTROS (2 columnas)
        # ========================================
        tgb.text("## 🔍 Filtros Interactivos", mode="md")
        
        with tgb.layout(columns="1 1"):
            # Columna 1: Rango de edad
            with tgb.part():
                tgb.text("### 📅 Rango de Edad", mode="md")
                
                # Edad mínima
                with tgb.layout(columns="1 3"):
                    tgb.text("Mínimo:")
                    tgb.text(value="{edad_min}", format="%.0f años")
                tgb.slider(value="{edad_min}", min=0, max=100, on_change=on_filter_change)
                
                # Edad máxima
                with tgb.layout(columns="1 3"):
                    tgb.text("Máximo:")
                    tgb.text(value="{edad_max}", format="%.0f años")
                tgb.slider(value="{edad_max}", min=0, max=100, on_change=on_filter_change)
            
            # Columna 2: Sexo
            with tgb.part():
                tgb.text("### ⚥ Sexo", mode="md")
                tgb.selector(
                    value="{sexo_sel}",
                    lov=["Todos", "Masculino", "Femenino"],
                    dropdown=True,
                    on_change=on_filter_change
                )
        
        # Comorbilidades (fila completa)
        tgb.text("### 🫀 Comorbilidades", mode="md")
        tgb.selector(
            value="{comorb_sel}",
            lov=[
                "DIABETES", "HIPERTENSION", "OBESIDAD", "ASMA",
                "EPOC", "INMUSUPR", "RENAL_CRONICA", "TABAQUISMO"
            ],
            multiple=True,
            on_change=on_filter_change
        )
        
        tgb.html("hr")
        
        # ========================================
        # GRÁFICOS DE EDAD (2 columnas)
        # ========================================
        tgb.text("## 📈 Análisis de Edad", mode="md")
        
        with tgb.layout(columns="1 1"):
            # Histograma
            with tgb.part():
                tgb.text("### Histograma de Edades", mode="md")
                tgb.chart(value="{crear_histograma(datos_grafico)}")
            
            # Boxplot
            with tgb.part():
                tgb.text("### Boxplot por Resultado", mode="md")
                tgb.chart(value="{crear_boxplot_edades(datos_grafico)}")
        
        # ========================================
        # GRÁFICO DE COMORBILIDADES
        # ========================================
        tgb.text("## 📊 Análisis de Comorbilidades", mode="md")
        tgb.chart(value="{crear_grafico_comorbilidades(datos_grafico)}")
        
        # ========================================
        # TABLA DE DATOS
        # ========================================
        tgb.text("## 📋 Tabla de Datos (Muestra)", mode="md")
        tgb.table(
            value="{datos_grafico.head(100) if datos_grafico is not None else None}",
            width="100%",
            height="400px"
        )
        
        # Footer con estadísticas
        with tgb.layout(columns="1 1"):
            tgb.text(
                value=lambda state: f"**📊 Total en base:** {state.metricas.get('total', 0) if state.metricas else 0} registros",
                mode="md"
            )
            tgb.text(
                value=lambda state: f"**🔍 Mostrando:** {len(state.datos_grafico) if state.datos_grafico is not None else 0} registros en muestra",
                mode="md"
            )
    
    return page


# ============================================
# EXPORTAR LA PÁGINA CONSTRUIDA
# ============================================
home_page = build_page()