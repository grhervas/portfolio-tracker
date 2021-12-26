import streamlit as st
from notion import NotionAPI  # type: ignore
from datetime import date, timedelta
import pandas as pd
import plotly.express as px
# import time


@st.experimental_singleton()
def init_notion_api():
    return NotionAPI()


@st.experimental_memo(suppress_st_warning=True)
def get_dataframes(_notion):
    data_load_state = st.text("Loading data...")

    df_tran = notion.get_transactions_df()
    df_pos = notion.get_his_positions_df(df_tran)
    # df_pos.columns.names = (None, None)
    # df_pos.index.name = None

    data_load_state.text("Loading data... done!")

    return df_tran, df_pos


st.title("Portfolio tracker")
st.write("""Here's my first attempt at creating a *nice*
            dashboard for my investments.""")

# Initialize Notion API
notion = init_notion_api()
# Load all data in dataframes
df_tran, df_pos = get_dataframes(notion)

# Selectors in left panel
sel_date = st.sidebar.date_input(
    "Seleccione fecha",
    date.today() - timedelta(days=1))

st.write('Fecha seleccionada:', sel_date)

# PART 1: Position info and KPIs
st.subheader("Posiciones")

# Important KPIs
col1, col2, col3 = st.columns(3)

value_metric = df_pos['Valor'].loc[pd.Timestamp(sel_date)].sum()
diff_1d_metric = (df_pos["Valor"].loc[pd.Timestamp(sel_date) - timedelta(days=1):pd.Timestamp(sel_date)]
                                 .sum(axis=1).diff()[-1])
pct_1d_metric = (df_pos["Valor"].loc[sel_date - timedelta(days=1):sel_date]
                                .sum(axis=1).pct_change()[-1] * 100)
col1.metric("Valor cartera",
            f"{value_metric:.2f}€",
            f"{diff_1d_metric:.2f}€ ({pct_1d_metric:.2f}%)")


# col2.metric("Balance total", delta=f"{}€")
col3.metric("Humidity", "86%", "4%")

# Area plot with historic value of position
area_fig = px.area(df_pos["Valor"],
                   labels={"value": "Valor (€)"},
                   title="Value of position")
st.plotly_chart(area_fig)




# # df1 = df.loc[["2021-08-24"],
# #              pd.IndexSlice[["Cantidad", "Adj Close", "Valor"], :]]
# df1 = df_pos.iloc[-31:]["Valor"]

# x = st.slider('x')  # 👈 this is a widget
# st.write(x, 'squared is', x * x)

# st.text_input("Your name", key="nombre")
# # You can access the value at any point with:
# st.session_state.nombre

# 'Starting a long computation...'

# # Add a placeholder
# latest_iteration = st.empty()
# bar = st.progress(0)

# for i in range(100):
#   # Update the progress bar with each iteration.
#   latest_iteration.text(f'Iteration {i+1}')
#   bar.progress(i + 1)
#   time.sleep(0.1)

# '...and now we\'re done!'


# option = st.selectbox(
#     "Which product to show?",
#     df_pos.columns.get_level_values("Producto").unique()
# )

# st.write(f"You selected: {option}")

# st.line_chart(df1)

# left_column, right_column = st.columns(2)
# # You can use a column just like st.sidebar:
# left_column.button('Press me!')

# # Or even better, call Streamlit functions inside a "with" block:
# with right_column:
#     chosen = st.radio(
#         'Sorting hat',
#         ("Gryffindor", "Ravenclaw", "Hufflepuff", "Slytherin"))
#     st.write(f"You are in {chosen} house!")

# # st.write(df)
# # st.dataframe(df) # Same as st.write(), dynamic table
# # st.table(df) # Static table
# if st.checkbox('Show dataframe'):
#     st.dataframe(df1.style.highlight_max(axis=0))
