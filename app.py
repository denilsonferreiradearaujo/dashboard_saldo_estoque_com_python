# import psycopg2
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

# ====================== Função para buscar dados no NeonDB ou MySQL ======================
@st.cache_data(show_spinner=True)
def load_data():
    
    # Aqui conexção com NEONDB
    # conn = psycopg2.connect(
    
    conn = mysql.connector.connect(
        host="localhost",
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "mb52data")
    )
    
    df = pd.read_sql('SELECT * FROM `INVENTARIO`;', conn)
    conn.close()
    
    # Renomear colunas para nomes amigáveis para NEONDB
    # df.rename(columns={
    #     "material": "material",
    #     "Texto breve material": "descricao",
    #     "TMat": "tmat",
    #     "Cen.": "centro",
    #     "Dep.": "deposito",
    #     "UMB": "unidadeMedida",
    #     "Moeda": "moeda",
    #     "Utilização livre": "utilizacaoLivre",
    #     "Val.utiliz.livre": "valUtilizLivre",
    #     "Bloqueado": "bloqueado",
    #     "Val.estoque bloq.": "valBloqueado",
    #     "Em contr.qualidade.": "contrQualidade",
    #     "Valor verif.qual.": "valContrQualidade",
    #     "Trânsito e TE": "transitoTE",
    #     "Val.em trâns.e Trf": "valTransitoTrf",
    #     "Nº estoque especial": "estoqueEspecial",
    #     "UNIDADE": "unidade",
    #     "TIPO 2": "tipo",
    #     "OPERAÇÃO": "operacao",
    #     "CIDADE": "cidade",
    #     "CLASSIF": "classificacao",
    #     "data_extracao": "data_extracao",
    #     "data_importacao": "data_importacao"
    # }, inplace=True)
    
    df.rename(columns={
        "MATERIAL": "material",
        "TEXTO_BREVE_MATERIAL": "descricao",
        "TMAT": "tmat",
        "CEN": "centro",
        "DEP": "deposito",
        "UMB": "unidadeMedida",
        "MOEDA": "moeda",
        "UTILIZACAO_LIVRE": "utilizacaoLivre",
        "VAL_UTILIZ_LIVRE": "valUtilizLivre",
        "BLOQUEADO": "bloqueado",
        "VAL_ESTOQUE_BLOQ": "valBloqueado",
        "EM_CONTR_QUALIDADE": "contrQualidade",
        "VALOR_VERIF_QUAL": "valContrQualidade",
        "TRANSITO_TE": "transitoTE",
        "VAL_EM_TRANSF_TRF": "valTransitoTrf",
        "NRO_ESTOQUE_ESP": "estoqueEspecial",
        "UNIDADE": "unidade",
        "TIPO": "tipo",
        "OPERACAO": "operacao",
        "CIDADE": "cidade",
        "CLASSIF": "classificacao",
        "DATA_EXTRACAO": "data_extracao",
        "DATA_IMPORTACAO": "data_importacao"
    }, inplace=True)


    # Criar colunas auxiliares
    df['valor_total'] = (
        df['valUtilizLivre'].fillna(0) +
        df['valBloqueado'].fillna(0) +
        df['valContrQualidade'].fillna(0)
    )
    
    df['quantidade_total'] = (
        df['utilizacaoLivre'].fillna(0) +
        df['bloqueado'].fillna(0) +
        df['contrQualidade'].fillna(0)
    )
    
    df['data_extracao'] = pd.to_datetime(df['data_extracao'])
    return df

df = load_data()


import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from mysql_conn import get_connection

# ====================== Função para buscar dados no MySQL local ======================
@st.cache_data(show_spinner=True)
def load_data():
    conn = get_connection()
    df = pd.read_sql("SELECT * FROM INVENTARIO;", conn)
    conn.close()
    
    # Renomear colunas para nomes amigáveis
    df.rename(columns={
        "MATERIAL": "material",
        "TEXTO_BREVE_MATERIAL": "descricao",
        "TMAT": "tmat",
        "CEN": "centro",
        "DEP": "deposito",
        "UMB": "unidadeMedida",
        "MOEDA": "moeda",
        "UTILIZACAO_LIVRE": "utilizacaoLivre",
        "VAL_UTILIZ_LIVRE": "valUtilizLivre",
        "BLOQUEADO": "bloqueado",
        "VAL_ESTOQUE_BLOQ": "valBloqueado",
        "EM_CONTR_QUALIDADE": "contrQualidade",
        "VALOR_VERIF_QUAL": "valContrQualidade",
        "TRANSITO_TE": "transitoTE",
        "VAL_EM_TRANSF_TRF": "valTransitoTrf",
        "NRO_ESTOQUE_ESP": "estoqueEspecial",
        "UNIDADE": "unidade",
        "TIPO": "tipo",
        "OPERACAO": "operacao",
        "CIDADE": "cidade",
        "CLASSIF": "classificacao",
        "DATA_EXTRACAO": "data_extracao",
        "DATA_IMPORTACAO": "data_importacao"
    }, inplace=True)

    # Criar colunas auxiliares
    df['valor_total'] = (
        df['valUtilizLivre'].fillna(0) +
        df['valBloqueado'].fillna(0) +
        df['valContrQualidade'].fillna(0)
    )
    
    df['quantidade_total'] = (
        df['utilizacaoLivre'].fillna(0) +
        df['bloqueado'].fillna(0) +
        df['contrQualidade'].fillna(0)
    )
    
    df['data_extracao'] = pd.to_datetime(df['data_extracao'])
    return df


# ====================== Layout do Streamlit ======================

st.set_page_config(page_title="Dashboard Inventário",page_icon="📊", layout="wide")
st.title("📊 Centros controlados por logística")

# Informativo da ultima atualização
result=df['data_importacao'].max()
st.markdown(f"###### Última atualização: {result.strftime('%d/%m/%Y %H:%M:%S')}")

# ====================== Sidebar ======================
# logo
st.sidebar.image("public/logo_claro.png", caption="Online Analitycs")

# Filtros
st.sidebar.header("Filtros")
tipo = st.sidebar.multiselect("Tipo", df['tipo'].dropna().unique(), default=[])
unidade = st.sidebar.multiselect("Unidade", df['unidade'].dropna().unique(), default=[])
operacao = st.sidebar.multiselect("Operação", df['operacao'].dropna().unique(), default=[])

# Aplicar filtros
df_filtered = df.copy()
if tipo:
    df_filtered = df_filtered[df_filtered['tipo'].isin(tipo)]
if unidade:
    df_filtered = df_filtered[df_filtered['unidade'].isin(unidade)]
if operacao:
    df_filtered = df_filtered[df_filtered['operacao'].isin(operacao)]

# Filtrar dados pela última data de extração
ultima_data = df_filtered['data_extracao'].max()
df_ultima_data = df_filtered[df_filtered['data_extracao'] == ultima_data]

#  Selecionar colunas no Analitico
df_selection = df.query(
    "tipo in @tipo & unidade in @unidade & operacao in @operacao"
)

def Home():
    # Cartões
    col1, col2, col3, col4 = st.columns(4, gap='large')
    
    with col1:
        st.info('Centros', icon="🏭")
        col1.metric('Total de Centros', df_ultima_data['centro'].nunique())
    
    with col2:
        st.info('SKU´s', icon="📦")
        col2.metric("Total de SKU´s", df_ultima_data['material'].nunique())
    
    with col3:
        st.info('Quantidade', icon="🔢")
        col3.metric("Quantidade Total", f"{df_ultima_data['quantidade_total'].sum():,.0f}")  
    
    with col4:
        st.info("Valor", icon="💲")
        col4.metric("Valor Total", f"R$ {df_ultima_data['valor_total'].sum():,.0f}")
    
    # Tabela Analítica
    st.subheader("📋 Analítico")
    with st.expander("Visualizar tabela"):
        showData=st.multiselect('Filter: ',df_ultima_data.columns, default=['unidade','tipo', 'material', 'descricao', 'centro', 'deposito','operacao','classificacao', 'quantidade_total', 'valor_total', 'data_extracao'])
        st.write(df_ultima_data[showData])
    
    # Linha divisória
    st.markdown("---")

    # ====================== Graficos ======================
    st.subheader("📈 Insights")
    col1, col2 = st.columns(2)
    
    # Gráfico 1 - Pizza: Valor por Unidade
    with col1:
        if not df_ultima_data.empty:
            df_unidades = df_ultima_data.groupby("unidade")["valor_total"].sum().reset_index()
            fig1 = px.pie(df_unidades, values="valor_total", names="unidade", hole=0.5,
                        title="Distribuição por Unidade")
            st.plotly_chart(fig1, use_container_width=True)

    # Gráfico 2 - Barras: Valor por Operação
    with col2:
        if not df_ultima_data.empty:
            df_op = df_ultima_data.groupby("operacao")["valor_total"].sum().reset_index()
            fig2 = px.bar(df_op, x="operacao", y="valor_total", title="Valor por Operação")
            st.plotly_chart(fig2, use_container_width=True)

    # Gráfico 3: Evolução temporal
    # Usar todas as datas para mostrar tendência
    df_time = df_filtered.groupby("data_extracao")["valor_total"].sum().reset_index()
    if not df_time.empty:
        fig3 = px.line(df_time, x="data_extracao", y="valor_total", markers=True,
                    title="Evolução do Valor Total")
        st.plotly_chart(fig3, use_container_width=True)

    # Gráfico 4: Valor por Centro
    df_centro = df_ultima_data.groupby("unidade")["valor_total"].sum().reset_index()
    if not df_centro.empty:
        fig4 = px.bar(df_centro, x="valor_total", y="unidade", orientation="h",
                    title="Valor por Unidade")
        st.plotly_chart(fig4, use_container_width=True)

    
    # ========== Indicador final ==========
    st.markdown("### 📊 Indicadores Gerais")
    total1, total2, total3, total4 = st.columns(4)

    with total1:
        qtd_centros = df_ultima_data['centro'].nunique()
        fig5 = go.Figure(go.Indicator(
            mode="number",
            value=qtd_centros,
            # number={"prefix": "R$"},
            title={"text": "Quantidade de centros"}
        ))
        st.plotly_chart(fig5, use_container_width=True)
        
    with total2:
        qtd_material = df_ultima_data['material'].nunique()
        fig5 = go.Figure(go.Indicator(
            mode="number",
            value=qtd_material,
            # number={"prefix": "R$"},
            title={"text": "Quantidade de SKU´s"}
        ))
        st.plotly_chart(fig5, use_container_width=True)
        
    with total3:
        quantidade_total = df_ultima_data['quantidade_total'].sum()
        fig5 = go.Figure(go.Indicator(
            mode="number",
            value=quantidade_total,
            # number={"prefix": "R$"},
            title={"text": "Quantidade total"}
        ))
        st.plotly_chart(fig5, use_container_width=True)
            
    with total4:
        valor_total = df_ultima_data['valor_total'].sum()
        fig5 = go.Figure(go.Indicator(
            mode="number",
            value=valor_total,
            number={"prefix": "R$"},
            title={"text": "Valor total (R$)"},
        ))
        st.plotly_chart(fig5, use_container_width=True)
     
Home()






