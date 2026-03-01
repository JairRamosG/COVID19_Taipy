from taipy.gui import Gui, State
import taipy.gui.builder as tgb
import plotly.express as px
import pandas as pd
import taipy as tp 

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