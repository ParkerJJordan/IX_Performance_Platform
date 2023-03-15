from dash import Dash, html, dcc, Input, Output
from dash.dash_table import DataTable
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ixp_app.services.cycles import CyclePerformance

pairlist = ['41IXA', '41IXB', '41IXC', 
            '41IXD', '41IXE', '41IXF', 
            '41IXG', '41IXH', '41IXI']
colors = {'background': '#696969',
          'text': '#ffffff',
          'green': '#1e5000',
          'gray': '#696969', 
          'white':'#ffffff'}

app = Dash(__name__)


# fig = px.line(df, x=df['Cycle'], y=df['Total Throughput'])
# fig.add_scatter(x=df['Cycle'], y=df['Final Primary Service Breakthrough Point'])
# fig.add_scatter(x=df['Cycle'], y=df['Syrup Throughput per Resin Volume'])
# fig.add_scatter(x=df['Cycle'], y=df['Acid Chemical Usage'])
# fig.add_scatter(x=df['Cycle'], y=df['Base Chemical Usage'])
# fig.add_scatter(x=df['Cycle'], y=df['Sweetwater Generation'])
# fig.add_scatter(x=df['Cycle'], y=df['Waste Water Generation'])


app.layout = html.Div([
    html.H1("Argo Ion Exchange Perfomance Platform", style={'textAlign':'left', 'background': colors['background'], 'color': colors['text']}),
    html.Div(children=[
        html.Div(children=[
            html.Label('Selected Pair:'),
            dcc.Dropdown(pairlist, '41IXA', id='pair_dropdown_input'),
        ], style={'padding': 10, 'flex': 1}),

        html.Div(children=[
            html.Label('Cycles Back: '),
            dcc.Input(value=10, type='int')
        ], style={'padding': 10, 'flex': 1})
    ]),
], style={'display': 'flex', 'flex-direction': 'row'})

# app.layout = html.Div(style={'backgroundColor': colors['background']}, 
#     children=[
#         html.H1(children='Hello Dash', style={'textAlign': 'center', 'color': colors['text']}),

#         html.Div(children='Dash: A web application framework for your data.', style={
#             'textAlign': 'center',
#             'color': colors['text']
#         })
# ])

@app.callback(
    Output('kpi_df', 'children'),
    Input('pair_dropdown_input','value'),
    Input('cycle_offset_input', 'value'))
def update_output(selected_pair, cycle_offset):
    kpi = CyclePerformance(pairname=selected_pair, cycle_offset=cycle_offset).kpis()
    # thru = CyclePerformance(pairname=selected_pair, cycle_offset=cycle_offset).throughput()
    return kpi



if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port=8700)
