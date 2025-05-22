import streamlit as st
import pandas as pd
import plotly.express as px
from databricks import sql
from datetime import datetime, timedelta

# 🔹 Configuração do Streamlit
st.set_page_config(page_title="Dashboard Databricks", layout="wide")

# 🔹 Título
st.title("📊 Dashboard de Atendimento - Notas e Tempo de Espera")

# 🔹 Configuração da conexão com Databricks
HOST = "dbc-6a9b798d-9256.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/31ee6c7460cbead5"
ACCESS_TOKEN = "dapi35df33ea3adaf82c8565b62005f6fcea"

# 🔹 Filtros
st.sidebar.header("📅 Filtros")
data_inicio = st.sidebar.date_input("Data de Início", datetime.now() - timedelta(days=7))
data_fim = st.sidebar.date_input("Data de Fim", datetime.now())

# Converter datas para o formato SQL (YYYY-MM-DD HH:MM:SS)
data_inicio_str = f"{data_inicio} 00:00:00"
data_fim_str = f"{data_fim} 23:59:59"

# 🔹 Função para buscar os dados de notas dos atendentes
@st.cache_data
def get_atendentes_data(start_date, end_date):
    try:
        conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()

        query = f"""
        SELECT 
            du.user_id,
            du.name AS nome_atendente,
            ROUND(AVG(fecsr.csat), 2) AS media_nota_atendente
        FROM headless_bi.client_q700zorent.fact_event_customer_support_request fecsr
        JOIN headless_bi.client_q700zorent.dim_user du ON fecsr.user_id = du.user_id
        WHERE fecsr.started_at BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY du.user_id, du.name
        ORDER BY media_nota_atendente DESC
        """

        cursor.execute(query)
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        return pd.DataFrame(dados, columns=colunas)

    except Exception as e:
        st.error(f"❌ Erro ao buscar dados: {e}")
        return pd.DataFrame()

# 🔹 Função para buscar o total de atendimentos humanos
@st.cache_data
def get_total_atendimentos(start_date, end_date):
    try:
        conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()

        query = f"""
        SELECT COUNT(*) AS total_atendimentos_humanos
        FROM headless_bi.client_q700zorent.dim_customer_support_request dcsr 
        WHERE 
            from_utc_timestamp(created_at, 'America/Sao_Paulo') >= '{start_date}'
            AND from_utc_timestamp(created_at, 'America/Sao_Paulo') <= '{end_date}'
            AND first_replied_by IS NOT NULL
        """

        cursor.execute(query)
        total_atendimentos = int(cursor.fetchone()[0])

        cursor.close()
        conn.close()

        return total_atendimentos

    except Exception as e:
        st.error(f"❌ Erro ao buscar atendimentos humanos: {e}")
        return 0

# 🔹 Função para buscar o tempo médio de espera
@st.cache_data
def get_tempo_espera(start_date, end_date):
    try:
        conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()

        query = f"""
        SELECT ROUND(AVG(waiting_time) / 60, 2) AS media_tempo_espera_minutos
        FROM headless_bi.client_q700zorent.fact_event_customer_support_request fecsr 
        WHERE started_at BETWEEN '{start_date}' AND '{end_date}'
        """

        cursor.execute(query)
        tempo_espera = round(cursor.fetchone()[0], 2) if cursor.fetchone()[0] else 0

        cursor.close()
        conn.close()

        return tempo_espera

    except Exception as e:
        st.error(f"❌ Erro ao buscar tempo médio de espera: {e}")
        return 0

# 🔹 Função para buscar atendimentos do início ao fim pelo mesmo agente
@st.cache_data
def get_atendimentos_por_agente(start_date, end_date):
    try:
        conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()

        query = f"""
        SELECT 
            du.user_id,
            du.name AS nome_agente,
            COUNT(fcsr.created_at) AS total_atendimentos_mesmo_agente
        FROM headless_bi.client_q700zorent.fact_customer_support_request fcsr
        JOIN headless_bi.client_q700zorent.dim_user du 
            ON fcsr.contacted_by = du.user_id
        WHERE 
            DATE(fcsr.created_at) >= DATE('{start_date}')
            AND DATE(fcsr.created_at) <= DATE('{end_date}')
            AND fcsr.contacted_by = fcsr.finished_by
        GROUP BY du.user_id, du.name
        ORDER BY total_atendimentos_mesmo_agente DESC
        """

        cursor.execute(query)
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        return pd.DataFrame(dados, columns=colunas)

    except Exception as e:
        st.error(f"❌ Erro ao buscar atendimentos por agente: {e}")
        return pd.DataFrame()

# 🔹 Função para buscar ocorrências por atendente
@st.cache_data
def get_ocorrencias_atendentes(start_date, end_date):
    try:
        conn = sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=ACCESS_TOKEN)
        cursor = conn.cursor()

        query = f"""
        SELECT 
            du.user_id,
            du.name AS nome_atendente,
            COUNT(fecsr.user_id) AS total_ocorrencias
        FROM headless_bi.client_q700zorent.fact_event_customer_support_request fecsr
        JOIN headless_bi.client_q700zorent.dim_user du 
            ON fecsr.user_id = du.user_id
        WHERE fecsr.finished_by = fecsr.user_id
          AND DATE(fecsr.started_at) BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY du.user_id, du.name
        ORDER BY total_ocorrencias DESC
        """

        cursor.execute(query)
        dados = cursor.fetchall()
        colunas = [desc[0] for desc in cursor.description]

        cursor.close()
        conn.close()

        return pd.DataFrame(dados, columns=colunas)

    except Exception as e:
        st.error(f"❌ Erro ao buscar dados de ocorrências: {e}")
        return pd.DataFrame()

# 🔹 Buscar os dados com os filtros aplicados
df_atendentes = get_atendentes_data(data_inicio_str, data_fim_str)
total_atendimentos = get_total_atendimentos(data_inicio_str, data_fim_str)
tempo_medio_espera = get_tempo_espera(data_inicio_str, data_fim_str)
total_por_agente = get_atendimentos_por_agente(data_inicio_str, data_fim_str)
df_ocorrencias = get_ocorrencias_atendentes(data_inicio_str, data_fim_str)

# 🔹 Exibir os Big Numbers
st.subheader("📊 Indicadores")
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(label="📞 Total de Atendimentos Humanos", value=f"{total_atendimentos}")

with col2:
    st.metric(label="⏳ Tempo Médio de Espera (min)", value=f"{tempo_medio_espera:.2f}")


# 🔹 Gráfico: Média das Notas por Atendente
if not df_atendentes.empty:
    st.subheader("📈 Média das Notas por Atendente")
    
    fig = px.bar(
        df_atendentes, 
        x="nome_atendente", 
        y="media_nota_atendente", 
        title="Média das Notas por Atendente",
        text_auto=True,
        labels={"nome_atendente": "Atendente", "media_nota_atendente": "Média da Nota"}
    )

    st.plotly_chart(fig)

# 🔹 Gráfico: Total de Ocorrências por Atendente
if not df_ocorrencias.empty:
    st.subheader("📊 Total de Ocorrências por Atendente")

    fig = px.bar(
        df_ocorrencias, 
        x="nome_atendente", 
        y="total_ocorrencias", 
        title="Total de Ocorrências por Atendente",
        text_auto=True,
        labels={"nome_atendente": "Atendente", "total_ocorrencias": "Total de Ocorrências"}
    )

    st.plotly_chart(fig)

df_atendimentos_por_agente = get_atendimentos_por_agente(data_inicio_str, data_fim_str)

# 🔹 Consolidar os dados em uma única DataFrame por agente
def consolidar_dados(df_nota, df_ocorrencia, df_completo):
    try:
        df_final = pd.merge(df_nota, df_ocorrencia, on=["user_id", "nome_atendente"], how="outer")
        df_final = pd.merge(df_final, df_completo, left_on=["user_id", "nome_atendente"], right_on=["user_id", "nome_agente"], how="outer")

        # Eliminar coluna duplicada
        df_final = df_final.drop(columns=["nome_agente"], errors='ignore')

        # Substituir NaNs por 0 onde for numérico
        for col in ['media_nota_atendente', 'total_ocorrencias', 'total_atendimentos_mesmo_agente']:
            if col in df_final.columns:
                df_final[col] = df_final[col].fillna(0)

        return df_final

    except Exception as e:
        st.error(f"❌ Erro ao consolidar dados: {e}")
        return pd.DataFrame()

# 🔹 Consolidar os DataFrames
df_consolidado = consolidar_dados(df_atendentes, df_ocorrencias, df_atendimentos_por_agente)

# 🔹 Exibir a tabela consolidada
if not df_consolidado.empty:
    st.subheader("📋 Dados Consolidados por Agente")
    st.dataframe(df_consolidado)