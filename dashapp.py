import dash
from dash import html
from dash import dcc
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from ixp_app.services.ixperformance import IXPerformance
from ixp_app.services.common import render_dataframe

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

ixpreform = IXPerformance()
cycle_table = render_dataframe(ixpreform.performancem, True)


app.layout = html.Div([html.H1("Sales KPIs")])

if __name__ == "__main__":
    app.run_server(debug=True)