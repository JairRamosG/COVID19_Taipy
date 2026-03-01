from taipy.gui import Gui
from src.pages.home import home_page

edad_min = 50

if __name__ == "__main__":
    gui = Gui(page = home_page)


    gui.run(
        use_reloader = True, 
        debug = True,
        title = "Dashboard Covid 19"
    )