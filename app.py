import streamlit as st
from notion import NotionAPI  # type: ignore
from datetime import date, timedelta
import pandas as pd
import numpy_financial as npf  # type: ignore
import plotly.express as px
# import time

PERIOD_OPTIONS = [
    "YTD", "1d", "1w", "1m", "3m", "6m",
    "1y", "3y", "5y", "10y", "MAX"]


@st.experimental_singleton()
def init_notion_api():
    return NotionAPI()


@st.experimental_memo(suppress_st_warning=True)
def get_data(_notion):
    data_load_state = st.text("Loading data...")

    df_tran = notion.get_transactions_df()
    products = notion.products
    df_pos = notion.get_his_positions_df(df_tran)
    # df_pos.columns.names = (None, None)
    # df_pos.index.name = None
    df_perf = notion.get_performance_df(df_tran, df_pos)

    data_load_state.text("Data loaded!")

    return df_tran, products, df_pos, df_perf


@st.experimental_memo()
def get_sunburst_fig(df_pos, products, sel_date):
    df = (df_pos.loc[pd.Timestamp(sel_date),
                    ["Cantidad", "Valor"]].unstack(level=0)
                                          .reset_index())
    df["Desc"] = df["Producto"].replace(
        products.set_index("ticker_yfinance")["Producto"]
                .to_dict())
    df["Tipo"] = ["Equity", "Bonds", "Equity", "Equity", "Cash"]
    df["Valor_perc"] = round(df["Valor"] / df["Valor"].sum() * 100, 2)

    fig = px.sunburst(df, path=["Tipo", "Producto"], values="Valor_perc",
                      title=f"Posición a {sel_date.strftime('%d/%m/%Y')}")
    fig.update_traces(hovertemplate="<b>%{label}</b><br>%{parent}<br>%{value}%")

    return fig


st.title("Portfolio tracker")
st.write("""Primer intento de crear algo *niiice*.""")

# Initialize Notion API (in theory, also cached)
notion = init_notion_api()
# Load all data in dataframes (cached function)
df_tran, products, df_pos, df_perf = get_data(notion)

# Selectors in left panel
# Selected date (actual, to display)
sel_date = st.sidebar.date_input(
    "Seleccione fecha",
    date.today() - timedelta(days=1))

# Selected interval/period (to calculate start date)
sel_period = st.sidebar.radio(
    "Seleccione período",
    PERIOD_OPTIONS,
    PERIOD_OPTIONS.index("MAX"))

if sel_period == "MAX":
    start_date = df_tran["Fecha"].min().date()
elif sel_period == "YTD":
    start_date = date(sel_date.year, 1, 1)
elif "d" in sel_period:
    d = int(sel_period.replace("d", ""))
    start_date = sel_date - timedelta(days=d)
elif "w" in sel_period:
    w = int(sel_period.replace("w", ""))
    start_date = sel_date - timedelta(days=w*7)
elif "m" in sel_period:
    m = int(sel_period.replace("m", ""))
    start_date = sel_date - timedelta(days=m*30)
elif "y" in sel_period:
    y = int(sel_period.replace("y", ""))
    start_date = sel_date - timedelta(days=y*365)


# PART 1: Position info and KPIs
st.subheader("Resumen posiciones")

# Important KPIs (3)
col1, col2, col3, col4 = st.columns(4)

# Portfolio value (and vs. Day-1)
value_portfolio = df_pos.loc[pd.Timestamp(sel_date), "Valor"].sum()
diff_1d_portfolio = (df_pos.loc[sel_date - timedelta(days=1):sel_date, "Valor"]
                           .sum(axis=1).diff()[-1])
pct_1d_portfolio = (df_pos.loc[sel_date - timedelta(days=1):sel_date, "Valor"]
                          .sum(axis=1).pct_change()[-1] * 100)
col1.metric("Valor cartera (vs día ant.)",
            f"{value_portfolio:.2f}€",
            f"{diff_1d_portfolio:.2f}€ ({pct_1d_portfolio:.2f}%)")

# Total Profit/Losses from beggining of times (% of deposits)
tot_deposits = df_tran.loc[df_tran["Fecha"] <= pd.Timestamp(sel_date),
                           "Ingresado"].iloc[-1]
tot_profits = value_portfolio - tot_deposits
pct_profits = tot_profits / tot_deposits * 100
col2.metric("Total G/P (vs aportado)",
            f"{tot_profits:.2f}€",
            f"{pct_profits:.2f}%")

# Time-weighted return (TWR)
twr = (df_perf.loc[start_date:sel_date, "ROR_daily"].cumprod() - 1)[-1] * 100
col3.metric("TWR",
            f"{twr:.2f}%")

# Money-weighted return (MWR)
s_mwr = - df_perf.loc[start_date:sel_date, "movimientos"]
# Not sure if this is right, but for first day include current
# position as cash outflow (as if it were a deposit, negative)
min_date = df_tran["Fecha"].min().date()
if start_date < min_date:
    s_mwr[min_date] = - df_perf.loc[pd.Timestamp(min_date), "valor act."]
else:
    s_mwr[start_date] = - df_perf.loc[pd.Timestamp(start_date), "valor act."]
s_mwr[sel_date] = (df_perf.loc[pd.Timestamp(sel_date), "valor act."]
                   - df_perf.loc[pd.Timestamp(sel_date), "movimientos"])
mwr = ((npf.irr(s_mwr) + 1)**365 - 1) * 100
col4.metric("MWR",
            f"{mwr:.2f}%")

st.write(f"Intervalo de fechas seleccionado: {start_date.strftime('%d/%m/%Y')}* - {sel_date.strftime('%d/%m/%Y')}")
st.write(f"**Solo aplica a Time-Weighted Return and Money-Weighted Return.*")


# PART 2: Asset Allocation
st.subheader("Asignación de activos")

col1, col2 = st.columns([3, 1])

# Area plot with historic value of position
area_fig = px.area(df_pos.loc[start_date:sel_date, "Valor"],
                   labels={"value": "Valor (€)"},
                   title=f"Histórico {start_date.strftime('%d/%m/%Y')} - {sel_date.strftime('%d/%m/%Y')}")
col1.plotly_chart(area_fig)

# Sunburst chart with current asset allocation
sunburst_fig = get_sunburst_fig(df_pos, products, sel_date)
col2.plotly_chart(sunburst_fig)


# PART 3: Asset Performance
st.subheader("Rendimiento")

df_rets = (
    (df_pos["Adj Close"].pct_change() + 1)
    .join(df_perf["ROR_daily"].rename("Cartera"))
)

line_fig = px.line((df_rets.loc[start_date:sel_date].cumprod() - 1) * 100,
                   labels={"value": "Rendimiento (%)",
                           "variable": "Producto"},
                   hover_data={"value": ":.2f"})

st.plotly_chart(line_fig)
