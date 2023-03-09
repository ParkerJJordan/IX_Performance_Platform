import dash
from dash import html
from dash import dcc
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
from ixp_app.services.cycles import CyclePerformance
from ixp_app.services.common import render_dataframe

pairname = '41IXA'
ix = CyclePerformance(pairname=pairname, cycle_offset=10)
df = ix.kpis()

app = dash.Dash(__name__)
app.layout = DataTable(id="table",
                          data=df.to_dict("records"),
                          columns=[{"name": i, "id": i} for i in df.columns])

#app.layout = html.Div([html.H1("Argo Ion Exchange Performance Platform")])

# app.layout = html.Div(
#     [
#         html.Div(
#             [
#                 html.H3("Cycle Analysis Table", style={"textAlign":"center"}),
#                 DataTable(id="table",
#                           data=ix.performance.to_dict('records'),
#                           columns=[{"name": i, "id": i} for i in ix.performance.columns]),
#             ],
#             style={"margin": 50},
#         ),
#     ],
#     className="row"
# )

if __name__ == "__main__":
    app.run_server(debug=True)