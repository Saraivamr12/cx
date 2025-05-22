import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql
from datetime import datetime, timedelta

# 🔹 Configuração do Streamlit
st.set_page_config(page_title="Dashboard Atendimento", layout="wide")

# 🔹 Criar abas no dashboard
aba_overview, aba_telefonia, aba_whatsapp = st.tabs(["📊 Overview", "📞 Telefonia", "💬 WhatsApp"])

# 🔹 Configuração da conexão com Databricks
CONNECTION_DETAILS = {
    "server_hostname": "dbc-6a9b798d-9256.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/31ee6c7460cbead5",
    "access_token": "dapi35df33ea3adaf82c8565b62005f6fcea"
}

# 🔹 Filtros na barra lateral
st.sidebar.header("📅 Filtros")
data_inicio = st.sidebar.date_input("Data de Início", datetime.now() - timedelta(days=7))
data_fim = st.sidebar.date_input("Data de Fim", datetime.now())

# 🔹 Função para carregar dados da planilha de telefonia
@st.cache_data
def load_telefonia_data():
    try:
        df = pd.read_excel("Relatório de Ligações Jan 25 - completo.xlsx", sheet_name="Calls")
        df["CallLocalTime"] = pd.to_datetime(df["CallLocalTime"], errors="coerce")
        df["Duration"] = pd.to_numeric(df["Duration"], errors="coerce")
        return df
    except Exception as e:
        st.error(f"❌ Erro ao carregar os dados de telefonia: {e}")
        return pd.DataFrame()

# 🔹 Função para buscar dados do Databricks
@st.cache_data
def execute_query(query):
    try:
        with sql.connect(
            server_hostname=CONNECTION_DETAILS["server_hostname"],
            http_path=CONNECTION_DETAILS["http_path"],
            access_token=CONNECTION_DETAILS["access_token"]
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                dados = cursor.fetchall()
                colunas = [desc[0] for desc in cursor.description]
                return pd.DataFrame(dados, columns=colunas)
    except Exception as e:
        st.error(f"❌ Erro na consulta ao Databricks: {e}")
        return pd.DataFrame()

# 🔹 Buscar dados de atendimentos humanos
@st.cache_data
def get_total_atendimentos(start_date, end_date):
    query = f"""
    SELECT COUNT(*) AS total_atendimentos_humanos
    FROM headless_bi.client_q700zorent.dim_customer_support_request 
    WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
    """
    df = execute_query(query)
    return df.iloc[0, 0] if not df.empty else 0

# 🔹 Buscar os dados
total_atendimentos = get_total_atendimentos(data_inicio, data_fim)
df_telefonia = load_telefonia_data()

# Filtrar os dados conforme as datas selecionadas
if not df_telefonia.empty:
    df_telefonia = df_telefonia[(df_telefonia["CallLocalTime"].dt.date >= data_inicio) & 
                                (df_telefonia["CallLocalTime"].dt.date <= data_fim)]

# 🔹 Aba Overview
with aba_overview:
    st.subheader("📊 Visão Geral do Atendimento")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(label="📞 Total de Atendimentos", value=f"{total_atendimentos}")
    if not df_telefonia.empty:
        total_chamadas = df_telefonia.shape[0]
        media_duracao = df_telefonia["Duration"].mean() / 60
        chamadas_abandonadas = df_telefonia[df_telefonia["Abandon"] == 1].shape[0]
        taxa_abandono = (chamadas_abandonadas / total_chamadas) * 100 if total_chamadas > 0 else 0
        nota_media = df_telefonia["NotaAtendimento"].mean()

        with col2:
            st.metric(label="📞 Total de Chamadas", value=f"{total_chamadas:,}".replace(",", "."))
        with col3:
            st.metric(label="⏳ Tempo Médio", value=f"{round(media_duracao)} Minutos")
        with col4:
            st.metric(label="📉 Taxa de Abandono", value=f"{taxa_abandono:.2f}%")

        # 🔹 Gráficos na Overview
        st.subheader("📊 Análises de Telefonia")

        # Volume de Atendimentos por Hora
        calls_per_hour = df_telefonia.groupby(df_telefonia['CallLocalTime'].dt.hour).size()
        fig1 = px.line(x=calls_per_hour.index, y=calls_per_hour.values,
                       title="Volume de Atendimentos por Hora",
                       labels={"x": "Horário", "y": "Quantidade"}, markers=True)
        st.plotly_chart(fig1, use_container_width=True)

        # Distribuição das Notas de Atendimento
        notas_freq = df_telefonia["NotaAtendimento"].value_counts().sort_index()
        fig2 = px.bar(x=notas_freq.index, y=notas_freq.values,
                      title="Distribuição das Notas de Atendimento",
                      labels={"x": "Nota", "y": "Quantidade"}, text_auto=True)
        st.plotly_chart(fig2, use_container_width=True)

        # Taxa de Abandono e Resolução
        labels_abandono = ["Abandonadas", "Concluídas"]
        values_abandono = [chamadas_abandonadas, total_chamadas - chamadas_abandonadas]
        fig3 = go.Figure(data=[go.Pie(labels=labels_abandono, values=values_abandono,
                                      hole=0.4, textinfo="percent+label",
                                      marker=dict(colors=["#6b6b6b", "#0979b0"],
                                                  line=dict(color="white", width=1)))])
        st.plotly_chart(fig3, use_container_width=True)

# 🔹 Aba WhatsApp
with aba_whatsapp:
    st.subheader("💬 Dados do WhatsApp")
    st.write("📌 Aqui serão adicionados os dados e gráficos relacionados a atendimentos via WhatsApp")