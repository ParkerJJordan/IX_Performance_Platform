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
    html.Div(children=[
        html.H1("Argo Ion Exchange Perfomance Platform", style={'textAlign':'left', 'background': colors['background'], 'color': colors['text']}),
        html.Div(children=[
            html.Label('Selected Pair:'),
            dcc.Dropdown(pairlist, '41IXA', id='pair_dropdown_input'),
        ], style={'padding': 10, 'flex': 1}),

        html.Div(children=[
            html.Label('Cycles Back: '),
            dcc.Input(value=10, type='int', id='cycle_offest_input')
        ], style={'padding': 10, 'flex': 1})
    ]),
    html.Div(
        html.Div([
            dcc.Graph(id='kpi_fig'),
        ], style={'padding': 10, 'flex': 1})
    )
], style={'display': 'flex', 'flex-direction': 'row'})


@app.callback(
    Output('kpi_fig', 'figure'),
    Input('pair_dropdown_input','value'),
    Input('cycle_offset_input', 'value'))
def update_kpi_fig(selected_pair, cycle_offset):
    print(selected_pair)
    print(cycle_offset)
    IXCP = CyclePerformance(pairname=selected_pair, cycle_offset=10)
    kpi = IXCP.kpis()
    #thru = IXCP.throughput()
    endcyc = int(max(kpi['Cycle']))
    startcyc = endcyc - cycle_offset

    kpi_fig = go.Figure()
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Syrup Throughput per Resin Volume'], name='Syrup Throughput per Resin Volume [mtds/cuft]'))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Final Primary Service Breakthrough Point'], name='Final Conductivity [mS/cm]', yaxis="y2"))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Acid Chemical Usage'], name='Acid Chemical Usage [kgds/mtds]'))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Base Chemical Usage'], name='Base Chemical Usage [kgds/mtds]'))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Sweetwater Generation'], name='Sweetwater Generation [m3/mtds]', yaxis="y3"))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Waste Water Generation'], name='Waste Water Generation [m3/mtds]', yaxis="y4"))
    
    kpi_fig.add_trace(
        go.Scatter(x=kpi['Cycle'], y=kpi['Total Water Usage'], name='Total Water Usage [m3/mtds]', yaxis="y5"))
        
    kpi_fig.update_layout(
        xaxis=dict(domain=[startcyc, endcyc]),
        yaxis=dict(
            title="yaxis title",),
        yaxis2=dict(
            title="yaxis2 title",
            overlaying="y",
            side="right",),
        yaxis3=dict(
            title="yaxis3 title", 
            anchor="free", 
            overlaying="y", 
            autoshift=True),
        yaxis4=dict(
            title="yaxis4 title",
            anchor="free",
            overlaying="y",
            autoshift=True,),
        yaxis5=dict(
            title="yaxis4 title",
            anchor="free",
            overlaying="y",
            autoshift=True,),
    )

    kpi_fig.update_layout(
        title_text=f'KPI Trends for Previous {cycle_offset} Cycles'
    )

    kpi_fig.update_layout(transition_duration=500)

    return kpi_fig



if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port=8700)
