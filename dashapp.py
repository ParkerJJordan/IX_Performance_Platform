from dash import Dash, html, dcc
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ixp_app.services.cycles import CyclePerformance
from ixp_app.services.common import render_dataframe

app = Dash(__name__)
pairname = '41IXA'
ix = CyclePerformance(pairname=pairname, cycle_offset=10)
df = ix.kpis()

fig = px.line(df, x=df['Cycle'], y=df['Total Throughput'])
fig.add_scatter(x=df['Cycle'], y=df['Final Primary Service Breakthrough Point'])
fig.add_scatter(x=df['Cycle'], y=df['Syrup Throughput per Resin Volume'])
fig.add_scatter(x=df['Cycle'], y=df['Acid Chemical Usage'])
fig.add_scatter(x=df['Cycle'], y=df['Base Chemical Usage'])
fig.add_scatter(x=df['Cycle'], y=df['Sweetwater Generation'])
fig.add_scatter(x=df['Cycle'], y=df['Waste Water Generation'])
fig.show()

# app.layout = DataTable(id="table",
#                           data=df.to_dict("records"),
#                           columns=[{"name": i, "id": i} for i in df.columns])

# app.layout = html.Div([html.H1("Argo Ion Exchange Performance Platform")])




# app.layout = html.Div(children=[
#     html.H1("Argo Ion Exchange Perfomance Platform", style={"textAlign":"center"}),

#     html.Div(children='''
#         Permforamnce Metrics of the Previous Cycles 
#     '''),
    
#     dcc.Graph(
#         id='kpi trends',
#         figure=fig
#     )
# ])


if __name__ == "__main__":
    app.run_server(debug=False)
