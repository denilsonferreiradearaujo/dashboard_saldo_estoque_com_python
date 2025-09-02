import asyncio
import pandas as pd
import dash
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
from prisma import Prisma

# ===== Função para buscar dados do Neon =====
async def fetch_data():
    db = Prisma()
    await db.connect()
    registros = await db.inventario.find_many()
    await db.disconnect()
    return [r.model_dump() for r in registros]

# ===== Carrega dados em DataFrame =====
df = pd.DataFrame(asyncio.run(fetch_data()))
df['valor_total'] = (
    df['valUtilizLivre'].fillna(0) +
    df['valBloqueado'].fillna(0) +
    df['valContrQualidade'].fillna(0)
)
df['data_extracao'] = pd.to_datetime(df['data_extracao'])
df['dia'] = df['data_extracao'].dt.day
df['mes'] = df['data_extracao'].dt.month

# ===== Inicializa Dash =====
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Dashboard Inventário"

# ===== Layout =====
app.layout = dbc.Container([
    html.H2("Dashboard Inventário - NeonDB", className="text-center my-3"),

    dbc.Row([
        dbc.Col(dcc.Graph(id="graph1"), md=6),
        dbc.Col(dcc.Graph(id="graph2"), md=6),
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(id="graph3"), md=6),
        dbc.Col(dcc.Graph(id="graph4"), md=6),
    ]),

    dbc.Row([
        dbc.Col(dcc.Graph(id="graph11"), md=12),
    ])
], fluid=True)

# ===== Callbacks =====
@app.callback(
    Output("graph1", "figure"),
    Input("graph1", "id")
)
def graph1(_):
    df1 = df.groupby("cidade")["valor_total"].sum().reset_index()
    fig = px.pie(df1, values="valor_total", names="cidade", hole=0.5,
                 title="Distribuição por Cidade")
    return fig

@app.callback(
    Output("graph2", "figure"),
    Input("graph2", "id")
)
def graph2(_):
    df2 = df.groupby("operacao")["valor_total"].sum().reset_index()
    fig = px.bar(df2, x="operacao", y="valor_total",
                 title="Distribuição por Operação")
    return fig

@app.callback(
    Output("graph3", "figure"),
    Input("graph3", "id")
)
def graph3(_):
    df3 = df.groupby("data_extracao")["valor_total"].sum().reset_index()
    fig = px.line(df3, x="data_extracao", y="valor_total", markers=True,
                  title="Evolução do Valor Total por Data")
    return fig

@app.callback(
    Output("graph4", "figure"),
    Input("graph4", "id")
)
def graph4(_):
    df4 = df.groupby("centro")["valor_total"].sum().reset_index()
    fig = px.bar(df4, x="valor_total", y="centro", orientation="h",
                 title="Valor Total por Centro")
    return fig

@app.callback(
    Output("graph11", "figure"),
    Input("graph11", "id")
)
def graph11(_):
    total_valor = df["valor_total"].sum()
    fig = go.Figure(go.Indicator(
        mode="number",
        value=total_valor,
        number={"prefix": "R$"},
        title={"text": "Valor Total do Inventário"}
    ))
    return fig

if __name__ == "__main__":
    app.run_server(debug=True)
