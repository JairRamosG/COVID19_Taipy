from taipy.gui import Gui, State
import taipy.gui.builder as tbg
import plotly.express as px
import pandas as pd
import taipy as tp 

with tbg.Page() as home_page:
    tbg.text("Dashboard COvid 19 México", mode = "md")
    tbg.text("En construcción...", mode = "md")