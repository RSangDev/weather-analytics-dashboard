"""
Interactive Weather Analytics Dashboard using Streamlit and Plotly - VERSÃO MELHORADA
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import yaml
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from data.fetcher import WeatherDataFetcher  # noqa
from processing.analyzer import WeatherDataProcessor  # noqa

# Page configuration
st.set_page_config(
    page_title="Weather Analytics Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS melhorado
st.markdown(
    """
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(120deg, #1f77b4, #2ecc71);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .alert-warning {
        background-color: #fff3cd;
        color: #856404; /* Texto marrom escuro para contraste */
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #ffeeba;
        margin-bottom: 10px;
    }
    .alert-critical {
        background-color: #f8d7da;
        color: #721c24; /* Texto vermelho escuro */
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #f5c6cb;
        margin-bottom: 10px;
    }
    .alert-info {
        background-color: #d1ecf1;
        color: #0c5460; /* Texto azul escuro */
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #bee5eb;
        margin-bottom: 10px;
    }
    div[data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: bold;
    }
    </style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=3600)
def load_config():
    """Load configuration from YAML file."""
    config_path = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_data(ttl=3600)
def fetch_weather_data(config):
    """Fetch weather data with caching."""
    fetcher = WeatherDataFetcher(config)
    raw_data = fetcher.fetch_all_cities(
        config["cities"], config["processing"]["forecast_days"]
    )
    return raw_data


def consolidate_alerts(alerts):
    """
    Consolida alertas para mostrar apenas um por cidade por dia.
    Agrupa alertas do mesmo tipo, cidade e dia, mostrando o valor máximo/mínimo.
    """
    if not alerts:
        return []

    df_alerts = pd.DataFrame(alerts)
    df_alerts["time"] = pd.to_datetime(df_alerts["time"])
    df_alerts["date"] = df_alerts["time"].dt.date

    consolidated = []

    # Agrupar por cidade, tipo e data
    grouped = df_alerts.groupby(["city", "type", "date"])

    for (city, alert_type, date), group in grouped:
        # Pegar o valor mais extremo do grupo
        if alert_type in ["high_temperature", "high_wind", "heavy_precipitation"]:
            max_value = group["value"].max()
            max_time = group.loc[group["value"].idxmax(), "time"]
            count = len(group)

            # Criar mensagem consolidada
            if count > 1:
                message = f"{group['message'].iloc[0]} (pico: {max_value:.1f}, {count}x no dia)"  # noqa
            else:
                message = group["message"].iloc[0]

            consolidated.append(
                {
                    "type": alert_type,
                    "city": city,
                    "date": date,
                    "time": max_time,
                    "value": max_value,
                    "count": count,
                    "message": message,
                }
            )
        else:
            # Para outros tipos, pegar apenas o primeiro
            consolidated.append(
                {
                    "type": alert_type,
                    "city": city,
                    "date": date,
                    "time": group["time"].iloc[0],
                    "value": group["value"].iloc[0],
                    "count": len(group),
                    "message": group["message"].iloc[0],
                }
            )

    # Ordenar por data (mais recente primeiro) e depois por severidade
    severity_order = {
        "high_temperature": 0,
        "high_wind": 1,
        "heavy_precipitation": 2,
        "low_temperature": 3,
    }

    consolidated_df = pd.DataFrame(consolidated)
    consolidated_df["severity"] = consolidated_df["type"].map(severity_order)
    consolidated_df = consolidated_df.sort_values(
        ["date", "severity"], ascending=[False, True]
    )

    return consolidated_df.to_dict("records")


def create_temperature_map(daily_df):
    """Create interactive temperature map."""
    latest_data = daily_df.groupby("city").last().reset_index()

    fig = px.scatter_mapbox(
        latest_data,
        lat="latitude",
        lon="longitude",
        size="temp_mean",
        color="temp_mean",
        hover_name="city",
        hover_data={
            "temp_mean": ":.1f",
            "temp_min": ":.1f",
            "temp_max": ":.1f",
            "latitude": False,
            "longitude": False,
        },
        color_continuous_scale="RdYlBu_r",
        size_max=30,
        zoom=3,
        height=600,
        title="🗺️ Mapa de Temperatura - Condições Atuais",
    )

    fig.update_layout(
        mapbox_style="open-street-map",
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        font=dict(size=12),
    )

    return fig


def create_temperature_trends(df, selected_cities):
    """Create temperature trend chart."""
    filtered_df = df[df["city"].isin(selected_cities)]

    fig = px.line(
        filtered_df,
        x="time",
        y="temperature_2m",
        color="city",
        title="📈 Tendências de Temperatura",
        labels={"temperature_2m": "Temperatura (°C)", "time": "Data/Hora"},
        height=500,
    )

    fig.update_layout(
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(size=12),
    )

    fig.update_traces(line=dict(width=2))

    return fig


def create_moving_average_chart(df, selected_city):
    """Create chart with actual temperature and moving average."""
    city_df = df[df["city"] == selected_city].copy()

    fig = go.Figure()

    # Temperatura real (mais transparente)
    fig.add_trace(
        go.Scatter(
            x=city_df["time"],
            y=city_df["temperature_2m"],
            name="Temperatura Real",
            mode="lines",
            line=dict(color="lightblue", width=1),
            opacity=0.6,
            fill="tozeroy",
            fillcolor="rgba(173, 216, 230, 0.2)",
        )
    )

    # Média móvel (destaque)
    fig.add_trace(
        go.Scatter(
            x=city_df["time"],
            y=city_df["temp_ma"],
            name="Média Móvel (3h)",
            mode="lines",
            line=dict(color="darkblue", width=3),
        )
    )

    fig.update_layout(
        title=f"📊 Temperatura & Média Móvel - {selected_city}",
        xaxis_title="Data/Hora",
        yaxis_title="Temperatura (°C)",
        hovermode="x unified",
        height=450,
        font=dict(size=12),
    )

    return fig


def create_weather_metrics_dashboard(daily_df, selected_city):
    """Create multi-metric dashboard for a city."""
    city_data = daily_df[daily_df["city"] == selected_city].sort_values("date")

    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "🌡️ Faixa de Temperatura",
            "🌧️ Precipitação",
            "💨 Velocidade do Vento",
            "☁️ Cobertura de Nuvens & Umidade",
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": True}],
        ],
    )

    # Temperature range com área preenchida
    fig.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["temp_max"],
            name="Temp Máx",
            line=dict(color="red", width=2),
            showlegend=True,
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["temp_min"],
            name="Temp Mín",
            line=dict(color="blue", width=2),
            fill="tonexty",
            fillcolor="rgba(100, 149, 237, 0.2)",
            showlegend=True,
        ),
        row=1,
        col=1,
    )

    # Precipitation com gradiente de cor
    colors = [
        "lightblue" if p < 10 else "blue" if p < 30 else "darkblue"
        for p in city_data["precipitation_total"]
    ]
    fig.add_trace(
        go.Bar(
            x=city_data["date"],
            y=city_data["precipitation_total"],
            name="Precipitação",
            marker_color=colors,
            showlegend=False,
        ),
        row=1,
        col=2,
    )

    # Wind speed com marcadores
    fig.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["wind_max"],
            name="Vento Máx",
            line=dict(color="green", width=2),
            mode="lines+markers",
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    # Cloud cover e humidity
    fig.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["cloud_cover_mean"],
            name="Nuvens",
            line=dict(color="gray", width=2),
            showlegend=True,
        ),
        row=2,
        col=2,
    )
    fig.add_trace(
        go.Scatter(
            x=city_data["date"],
            y=city_data["humidity_mean"],
            name="Umidade",
            line=dict(color="cyan", width=2, dash="dash"),
            showlegend=True,
        ),
        row=2,
        col=2,
        secondary_y=True,
    )

    # Atualizar labels
    fig.update_xaxes(title_text="Data", row=2, col=1)
    fig.update_xaxes(title_text="Data", row=2, col=2)
    fig.update_yaxes(title_text="Temperatura (°C)", row=1, col=1)
    fig.update_yaxes(title_text="Precipitação (mm)", row=1, col=2)
    fig.update_yaxes(title_text="Vento (km/h)", row=2, col=1)
    fig.update_yaxes(title_text="Cobertura (%)", row=2, col=2)
    fig.update_yaxes(title_text="Umidade (%)", row=2, col=2, secondary_y=True)

    fig.update_layout(
        height=700,
        showlegend=True,
        font=dict(size=11),
        title_text=f"📊 Dashboard Completo - {selected_city}",
    )

    return fig


def create_alert_summary_chart(alerts_df):
    """Cria gráfico de resumo de alertas."""
    if alerts_df.empty:
        return None

    # Contar alertas por tipo
    alert_counts = alerts_df["type"].value_counts()

    # Mapear nomes amigáveis
    type_names = {
        "high_temperature": "Temperatura Alta",
        "low_temperature": "Temperatura Baixa",
        "high_wind": "Vento Forte",
        "heavy_precipitation": "Chuva Intensa",
    }

    alert_counts.index = alert_counts.index.map(lambda x: type_names.get(x, x))

    # Cores por tipo
    colors = {
        "Temperatura Alta": "#ff6b6b",
        "Temperatura Baixa": "#4dabf7",
        "Vento Forte": "#51cf66",
        "Chuva Intensa": "#339af0",
    }

    fig = go.Figure(
        data=[
            go.Bar(
                x=alert_counts.index,
                y=alert_counts.values,
                marker_color=[colors.get(name, "#gray") for name in alert_counts.index],
                text=alert_counts.values,
                textposition="auto",
            )
        ]
    )

    fig.update_layout(
        title="📊 Resumo de Alertas por Tipo",
        xaxis_title="Tipo de Alerta",
        yaxis_title="Quantidade",
        height=300,
        showlegend=False,
    )

    return fig


def main():
    """Main dashboard application."""

    # Header
    st.markdown(
        '<div class="main-header">🌤️ Weather Analytics Dashboard</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div style='text-align: center; color: #666; margin-bottom: 2rem;'>
    Monitoramento climático em tempo real para as principais cidades brasileiras
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Load configuration
    with st.spinner("🔄 Carregando configuração..."):
        config = load_config()

    # Sidebar
    st.sidebar.title("⚙️ Configurações")
    st.sidebar.markdown("---")

    # Refresh data button
    if st.sidebar.button(
        "🔄 Atualizar Dados", type="primary", use_container_width=True
    ):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Informações")
    st.sidebar.info(
        f"""
    **Cidades Monitoradas:** {len(config['cities'])}
    **Previsão:** {config['processing']['forecast_days']} dias
    **Atualização:** A cada hora
    **Fonte:** Open-Meteo API
    """
    )

    # Fetch and process data
    with st.spinner("🌐 Buscando dados meteorológicos..."):
        raw_data = fetch_weather_data(config)

    if not raw_data:
        st.error("❌ Falha ao buscar dados. Por favor, tente novamente mais tarde.")
        st.stop()

    processor = WeatherDataProcessor(config)

    with st.spinner("⚙️ Processando dados..."):
        df = processor.raw_to_dataframe(raw_data)
        df = processor.calculate_moving_averages(df)
        df = processor.detect_anomalies(df)
        daily_df = processor.aggregate_daily_stats(df)
        alerts = processor.generate_alerts(df)
        stats = processor.get_summary_statistics(df)

    # Consolidar alertas
    consolidated_alerts = consolidate_alerts(alerts)

    # Summary metrics com melhor design
    st.markdown("### 📊 Estatísticas Gerais")
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    with col1:
        st.metric(
            "🏙️ Cidades", stats["total_cities"], help="Número de cidades monitoradas"
        )

    with col2:
        st.metric(
            "🌡️ Temp Média",
            f"{stats['avg_temperature']:.1f}°C",
            help="Temperatura média em todas as cidades",
        )

    with col3:
        st.metric(
            "🔥 Máxima",
            f"{stats['max_temperature']:.1f}°C",
            delta=f"+{stats['max_temperature'] - stats['avg_temperature']:.1f}°C",
            help="Temperatura máxima registrada",
        )

    with col4:
        st.metric(
            "❄️ Mínima",
            f"{stats['min_temperature']:.1f}°C",
            delta=f"{stats['min_temperature'] - stats['avg_temperature']:.1f}°C",
            delta_color="inverse",
            help="Temperatura mínima registrada",
        )

    with col5:
        anomaly_delta = (  # noqa
            "normal" if stats["anomalies_detected"] == 0 else "attention"
        )  # noqa
        st.metric(
            "⚠️ Anomalias",
            stats["anomalies_detected"],
            help="Anomalias de temperatura detectadas",
        )

    with col6:
        alert_delta = "normal" if len(consolidated_alerts) == 0 else "attention"  # noqa
        st.metric(
            "🚨 Alertas", len(consolidated_alerts), help="Alertas meteorológicos ativos"
        )

    st.markdown("---")

    # Alertas section melhorado
    if consolidated_alerts:
        st.markdown("### ⚠️ Alertas Meteorológicos Ativos")

        # Criar DataFrame para visualização
        alerts_df = pd.DataFrame(consolidated_alerts)

        # Criar duas colunas: gráfico e lista
        col_chart, col_list = st.columns([1, 2])

        with col_chart:
            # Gráfico de resumo de alertas
            summary_chart = create_alert_summary_chart(alerts_df)
            if summary_chart:
                st.plotly_chart(summary_chart, use_container_width=True)

        with col_list:
            # Filtros de alertas
            alert_types = ["Todos"] + sorted(alerts_df["type"].unique().tolist())
            selected_alert_type = st.selectbox(
                "🔍 Filtrar por tipo:", alert_types, key="alert_filter"
            )

            # Filtrar alertas
            if selected_alert_type != "Todos":
                filtered_alerts = [
                    a for a in consolidated_alerts if a["type"] == selected_alert_type
                ]
            else:
                filtered_alerts = consolidated_alerts

            # Mostrar contador
            st.markdown(f"**{len(filtered_alerts)} alerta(s) encontrado(s)**")

        # Display alerts de forma organizada
        st.markdown("#### 📋 Lista de Alertas")

        # Mapear tipos para ícones e cores
        alert_icons = {
            "high_temperature": "🔥",
            "low_temperature": "❄️",
            "high_wind": "💨",
            "heavy_precipitation": "🌧️",
        }

        alert_colors = {
            "high_temperature": "alert-critical",
            "low_temperature": "alert-info",
            "high_wind": "alert-warning",
            "heavy_precipitation": "alert-warning",
        }

        # Mostrar até 15 alertas (para não poluir)
        display_limit = 15
        for i, alert in enumerate(filtered_alerts[:display_limit], 1):
            icon = alert_icons.get(alert["type"], "⚠️")
            css_class = alert_colors.get(alert["type"], "alert-info")
            date_str = alert["date"].strftime("%d/%m/%Y")

            st.markdown(
                f"""
                <div class='{css_class}'>
                    <strong>{icon} {alert['city']}</strong> - {date_str}<br>
                    {alert['message']}
                </div>
            """,
                unsafe_allow_html=True,
            )

        if len(filtered_alerts) > display_limit:
            st.info(f"➕ E mais {len(filtered_alerts) - display_limit} alertas...")

    else:
        st.success("✅ Nenhum alerta meteorológico ativo no momento")

    st.markdown("---")

    # Temperature Map
    st.markdown("### 🗺️ Mapa de Temperatura")
    temp_map = create_temperature_map(daily_df)
    st.plotly_chart(temp_map, use_container_width=True)

    st.markdown("---")

    # Detailed Analysis
    st.markdown("### 📈 Análise Detalhada")

    cities_list = sorted(df["city"].unique())

    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📊 Tendências",
            "📉 Médias Móveis",
            "🎯 Dashboard Completo",
            "📋 Dados Brutos",
        ]
    )

    with tab1:
        st.markdown("#### Comparação de Temperaturas entre Cidades")
        selected_cities_trend = st.multiselect(
            "Selecione as cidades para comparar:",
            cities_list,
            default=cities_list[:3],
            key="trend_cities",
        )

        if selected_cities_trend:
            trend_chart = create_temperature_trends(df, selected_cities_trend)
            st.plotly_chart(trend_chart, use_container_width=True)

            # Estatísticas rápidas
            st.markdown("##### 📊 Estatísticas Rápidas")
            stats_cols = st.columns(len(selected_cities_trend))
            for i, city in enumerate(selected_cities_trend):
                city_data = df[df["city"] == city]
                with stats_cols[i]:
                    st.metric(
                        city,
                        f"{city_data['temperature_2m'].mean():.1f}°C",
                        f"±{city_data['temperature_2m'].std():.1f}°C",
                    )

    with tab2:
        st.markdown("#### Análise de Média Móvel")
        selected_city_ma = st.selectbox(
            "Selecione a cidade:", cities_list, key="ma_city"
        )

        ma_chart = create_moving_average_chart(df, selected_city_ma)
        st.plotly_chart(ma_chart, use_container_width=True)

        # Explicação
        st.info(
            """
        📝 **Sobre a Média Móvel:**
        A linha azul escura representa a média móvel de 3 horas,
        suaviza as flutuações
        de curto prazo e ajuda a identificar tendências gerais de temperatura.
        """
        )

    with tab3:
        st.markdown("#### Dashboard Multi-Métrico")
        selected_city_metrics = st.selectbox(
            "Selecione a cidade:", cities_list, key="metrics_city"
        )

        metrics_dashboard = create_weather_metrics_dashboard(
            daily_df, selected_city_metrics
        )
        st.plotly_chart(metrics_dashboard, use_container_width=True)

    with tab4:
        st.markdown("#### Dados Completos")

        # Filtros
        col_filter1, col_filter2 = st.columns(2)
        with col_filter1:
            filter_cities = st.multiselect(
                "Filtrar por cidade:",
                cities_list,
                default=cities_list,
                key="filter_cities",
            )

        with col_filter2:
            show_anomalies_only = st.checkbox(  # noqa
                "Mostrar apenas anomalias", value=False
            )  # noqa

        # Aplicar filtros
        filtered_data = daily_df[daily_df["city"].isin(filter_cities)]

        # Mostrar dados
        st.dataframe(filtered_data, use_container_width=True, height=400)

        # Download button
        csv = filtered_data.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"weather_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 2rem 0;'>
            <strong>Data Source:</strong> <a href="https://open-meteo.com"
            target="_blank">Open-Meteo API</a><br>
            <strong>Update Frequency:</strong> Hourly |
            <strong>Coverage:</strong> 20 major Brazilian cities<br>
            <em>Developed with Streamlit, Pandas, and Plotly</em>
        </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
