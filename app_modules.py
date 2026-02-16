from fasthtml.common import *
from app import rt, nav_links, app
from fastapi.staticfiles import StaticFiles

app.mount("/images", StaticFiles(directory="images"), name="images")

def module_page():
    layout_style = Style("""
    :root{--col:30vw;--gap-x:20vw;--gap-y:15vh;--row:30vh}
    #columns{
    display:grid;
    grid: var(--row) / repeat(2,var(--col)); /* two cols */
    gap: var(--gap-y) var(--gap-x);
    grid-auto-rows: var(--row);             /* <-- make all rows same height */
    place-content:center;
    place-items:stretch;                     /* items fill the row height */
    }
    #columns>*{min-height:0;} #optional: prevent tall content from forcing growth
    """, id="layout-state")

    image_style = Style("""
    .box-image-class {
        display:block;
        opacity:0.7;
    }
    .image-card {
        background:rgba(255,255,255,.1);
        backdrop-filter:blur(4px);
        border-radius:16px;
        overflow:hidden;    /* this is what clips the image to rounded corners */
        width:100%;
        height:100%;
    }
    .card {
        height:100%;
        display:flex;
        flex-direction:column;
    }
    .card-text {
        margin-bottom:8px;
    }
    .card-body {
        flex:1;                   /* image area takes remaining height */
    }
    """)

    image_class = "box-image-class"

    card = lambda txt, image, link: Div(
        Div(txt, cls="card-text"),
        Div(
            A(Img(src=image, cls=image_class), href=link),
            cls="image-card card-body",
        ),
        cls="card"
    )
    
    return Div (
        layout_style,
        nav_links(),
        image_style,
        Div(
            card("Halogen Lab Graph", "/images/HalogenLabGraph.png", "http://10.6.16.28:8888/notebooks/jupyter_notebooks/HalogenGraph.ipynb"),
            card("Capacitance Logger", "/images/CapacitanceLogger.png", "http://10.6.16.28:8888/notebooks/jupyter_notebooks/Capacitance_Logger.ipynb"),
            card("Item 3", "/images/HalogenLabGraph.png", "http://10.6.16.28:8888/notebooks/jupyter_notebooks/HalogenGraph.ipynb"),
            card("Item 4", "/images/HalogenLabGraph.png", "http://10.6.16.28:8888/notebooks/jupyter_notebooks/HalogenGraph.ipynb"),
            id="columns"
        ),
        style=(
            "min-height:100vh; margin:0; padding:24px;"
            "display:grid; place-items:center; text-align:center;"
            "font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;"
            "background:linear-gradient(135deg,#38bdf8,#1e3a8a); color:white;"
        ),
    )

@rt("/modules")
def module_home():
    return module_page()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("modules:app", host="127.0.0.1", port=8000, reload=False)