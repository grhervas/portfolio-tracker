import pytest
from notion import NotionAPI
from datetime import date, timezone
import pandas as pd


@pytest.fixture
def notion():
    return NotionAPI()

@pytest.fixture
def df_tran(notion):
    return notion.get_transactions_df()

@pytest.fixture
def df_qty(df_tran):
    date_idx = pd.date_range(df_tran["Fecha"].dt.date.min(), date.today(), name="Fecha")

    df_qty = df_tran.loc[df_tran["Tipo"].isin(["Compra", "Venta"])] \
        .pivot(index="Fecha", columns="Producto", values="Unidades") \
        .sort_index().cumsum().fillna(method="ffill")
    df_qty = df_qty.groupby(df_qty.index.date).tail(1)
    df_qty = df_qty.set_index(df_qty.index.date).reindex(date_idx).fillna(method="ffill")

    return df_qty 

def test_query_db_not_empty(notion):
    assert bool(notion.query_db())

def test_df_tran_dates_are_utc(df_tran):
    assert df_tran["Fecha"].dt.tz is timezone.utc

@pytest.mark.parametrize("values_at_20221013", [
    {
        "Iberdrola S.A.": 347.0,
        "iShares Core Government Bond UCITS ETF EURO (Dist)": 1.0,
        "iShares Core MSCI EM IMI UCITS ETF USD (Acc)": 15.0,
        "iShares Core MSCI World UCITS ETF USD (Acc)": 24.0,
        "iShares Global Clean Energy UCITS ETF USD (Dist)": 19.0
    }
])
def test_df_qty(df_qty, values_at_20221013):
    assert all([df_qty.loc["2022-10-13"][key] == value 
                for key, value in values_at_20221013.items()])

# def test_df_tran_non_negative_cash(notion):
#     df_tran = notion.get_transactions_df()
