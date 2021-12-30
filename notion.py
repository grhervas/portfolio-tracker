import config  # type: ignore
import requests
from typing import Union
from datetime import datetime, date
import pandas as pd
import yfinance as yf
import pandas_datareader.data as web
from pprint import pprint

NOTION_URL = "https://api.notion.com/v1/databases/"
NOTION_VERSION = "2021-08-16"
DATABASE_ID = "aa1f83615ff945239302887264317370"

MARKETS_DICT = {"EAM": "AS", "XET": "DE"}
AV_API_KEY = "ILZ89DS0WFHPSG2L"


class NotionAPI():
    """
    A class to use Notion API and get data from Notion's databases.

    Methods
    -------
    ```python
    retrieve_db(token=secrets.INT_TOKEN, version=NOTION_VERSION)
    ```
        Retrieves whole database
    ```python
    query_db(token=secrets.INT_TOKEN, version=NOTION_VERSION)
    ```
        Queries database to get specific results
    """

    def __init__(self):
        pass

    def retrieve_db(self, token: str = config.INT_TOKEN,
                    version: str = NOTION_VERSION) -> dict:
        """
        Retrieves whole database using [Notion's API GET method](https://developers.notion.com/reference/retrieve-a-database/).

        Parameters
        ----------
        token : str, default=secrets.INT_TOKEN
            The secret integration token provided by Notion API needed in
            the CURL header. Keep it safe!
        version : str, default=NOTION_VERSION
            The Notion API version needed in the CURL header.

        Returns
        -------
        json
            The GET response from CURL in json format.

        Raises
        ------
        ConnectionError
            If the response status from the CURL request is different from 200.
        """

        db_url = NOTION_URL + DATABASE_ID
        header = {"Authorization": token, "Notion-Version": version}
        response = requests.get(db_url, headers=header)
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError(f"Response status: {response.status_code}")

    def query_db(self, token: str = config.INT_TOKEN,
                 version: str = NOTION_VERSION) -> dict:
        """
        Queries database for specific results using [Notion's API POST method](https://developers.notion.com/reference/post-database-query).

        Parameters
        ----------
        token : str, default=secrets.INT_TOKEN
            The secret integration token provided by Notion API needed in
            the CURL header. Keep it safe!
        version : str, default=NOTION_VERSION
            The Notion API version needed in the CURL header.

        Returns
        -------
        json
            The POST response from CURL in json format.

        Raises
        ------
        ConnectionError
            If the response status from the CURL request is different from 200.
        """

        db_url = NOTION_URL + DATABASE_ID + "/query"
        header = {"Authorization": token, "Notion-Version": version}
        response = requests.post(db_url, headers=header)
        if response.status_code == 200:
            return response.json()
        else:
            raise ConnectionError(f"Response status: {response.status_code}")

    def _extract_plain_text(self, rich_text_object: list) -> Union[str, None]:
        """
        From Notion's `rich_text` type get `plain_text` type

        3 cases are contemplated:
            - Cell is empty: this translates in empty list in schema -> returns None
            - Cell contains some text: list of 1 element in schema -> returns str
            - ¿?: list of >1 element in schema -> raises Error

        Parameters
        ----------
        ```python
        rich_text_object : list
        ```
            List containing Notion's `rich_text` type within `results` json schema

        Returns
        -------
        ```python
        str
        ```
            String object containing the cell plain text

        Raises
        ------
        ```python
        ValueError
        ```
            If object is empty or contains more than 1 element
        """

        if not rich_text_object:
            return None
        elif len(rich_text_object) == 1:
            return rich_text_object[0]["plain_text"]
        else:
            raise ValueError(f"More than 1 element in list!")

    def get_transactions_df(self) -> pd.DataFrame:
        """
        Returns `pandas.DataFrame` from Notion's database.

        Iterates through results from CURL response (following json schema)
        and applies appropiate format to each field depending on datatype.

        Returns
        -------
        ```python
        pandas.DataFrame
        ```
            The tabulated database result.
        """

        data = self.query_db()
        records = []
        for entry in data["results"]:
            record = {}
            for col, values in entry["properties"].items():
                # # TODO: wait for Streamlit to be compatible with Python 3.10
                # # in order to use match-case statement
                # match col:
                #     case "Tipo":
                #         record[col] = values["select"]["name"]
                #     case "Descripción":
                #         record[col] = self._extract_plain_text(values["rich_text"])
                #     case "Valor":
                #         record[col] = values["number"]
                #     case "Fecha":
                #         record[col] = datetime.strptime(values["date"]["start"],
                #                                         "%Y-%m-%d").date()
                #     case "Producto":
                #         record[col] = self._extract_plain_text(values["rich_text"])
                #     case "Unidades":
                #         record[col] = values["number"]
                #     case "Tasa":
                #         record[col] = values["number"]
                #     case "Símbolo":
                #         record[col] = self._extract_plain_text(values["rich_text"])
                #    # case _:
                #    #     pass

                # "rich_text" datatype
                if col in ["Producto", "ISIN", "Bolsa", "Centro ejecución",
                           "Símbolo", "Descripción"]:
                    record[col] = self._extract_plain_text(values["rich_text"])
                # "date" datatype
                elif col == "Fecha":
                    record[col] = datetime.strptime(values["date"]["start"],
                                                    "%Y-%m-%d").date()
                # "select" datatype
                elif col == "Tipo":
                    record[col] = values["select"]["name"]
                # "number" datatype
                elif col in ["Unidades", "Valor", "Tasa"]:
                    record[col] = values["number"]

            records.append(record)

        df = pd.DataFrame.from_records(records)

        # Create auxiliary columns for calculating Cash, Cumulative Deposits, Withdraws, etc.
        # df_tran_mod = df_tran.copy()
        # Order by ascending dates for computing cumulative values correctly
        df["Fecha"] = pd.to_datetime(df["Fecha"], format="%Y-%m-%d")
        df = df.sort_values("Fecha")

        df["Ingresado"] = abs(df.loc[df["Tipo"] == "Ingreso", "Valor"]
                                .cumsum().reindex(df.index).fillna(method="ffill").fillna(0.0))
        df["Retirado"] = abs(df.loc[df["Tipo"] == "Retirada", "Valor"]
                               .cumsum().reindex(df.index).fillna(method="ffill").fillna(0.0))
        df["Total"] = abs(df["Unidades"] * df["Valor"])
        df["Comprado"] = abs(df.loc[df["Tipo"] == "Compra", "Total"]
                               .cumsum().reindex(df.index).fillna(method="ffill").fillna(0.0))
        df["Vendido"] = abs(df.loc[df["Tipo"] == "Venta", "Total"]
                              .cumsum().reindex(df.index).fillna(method="ffill").fillna(0.0))
        df["Dividendos acumulados"] = abs(df.loc[df["Tipo"] == "Dividendo", "Valor"]
                                            .cumsum().reindex(df.index).fillna(method="ffill").fillna(0.0))
        df["Costes acumulados"] = abs(df["Tasa"].cumsum().fillna(method="ffill").fillna(0.0))

        df["Efectivo"] = (df[["Ingresado", "Dividendos acumulados", "Vendido"]].sum(axis=1) -
                          df[["Retirado", "Comprado", "Costes acumulados"]].sum(axis=1))

        # Get product info from transactions
        self.products = (df[["Producto", "ISIN", "Bolsa",
                             "Centro ejecución", "Símbolo"]].drop_duplicates()
                                                            .dropna())

        return df[["Fecha", "Tipo", "Producto", "ISIN", "Bolsa", "Centro ejecución",
                   "Símbolo", "Descripción", "Unidades", "Valor", "Tasa", "Ingresado",
                   "Retirado", "Total", "Comprado", "Vendido", "Dividendos acumulados",
                   "Costes acumulados", "Efectivo"]]

    def get_his_positions_df(self, df_tran: pd.DataFrame, format_out: str = "wide",
                             markets_dict: dict = MARKETS_DICT,
                             av_api_key: str = AV_API_KEY) -> pd.DataFrame:
        """
        Returns `pandas.DataFrame` with whole historic of open positions

        Contains info about number of open positions, price, value and other\n
        stock info from *Yahoo! Finance* (module yfinance)

        Parameters
        ----------
        ```python
        df_tran : pandas.DataFrame
        ```
            Dataframe containing list of transactions (obtained from `get_transactions_df()`)
        ```python
        format_out : str, default="wide"
        ```
            Indicator to return DataFrame in `wide` (default, index=Fecha), `long`\n
            (index=Fecha-Producto-Métrica) or `mixed` (index=Fecha-Producto) format

        Returns
        -------
        ```python
        pandas.DataFrame
        ```
            The tabulated result with dates from beginning to today as indexes and different \n
            financial products as columns.
        """

        # Generate date index
        date_idx = pd.date_range(df_tran["Fecha"].min(), date.today(), name="Fecha")

        # Invert "Buy" units sign (for those that are recorded as positive)
        df_tran.loc[(df_tran["Tipo"] == "Venta") & (df_tran["Unidades"] > 0),
                    "Unidades"] *= -1

        # Pivot transactions dataframe to get as index the date_range and the products
        # as columns, propagating last valid observations
        df_qty = (df_tran.loc[df_tran["Tipo"].isin(["Compra", "Venta"])]
                         .pivot(index="Fecha", columns="Producto", values="Unidades")
                         .sort_index().cumsum().reindex(date_idx).fillna(method="ffill"))

        # Create ticker symbol compatible with yfinance
        self.products["ticker_yfinance"] = (self.products["Símbolo"] + "." +
                                            self.products["Bolsa"].replace(markets_dict))
        # Create MultiIndex with renamed columns to YFinance compatible tickets
        prod_col_level = df_qty.rename(columns=self.products.set_index("Producto")["ticker_yfinance"]
                                                            .to_dict()).columns
        df_qty.columns = pd.MultiIndex.from_product([["Cantidad"], prod_col_level],
                                                    names=["Métrica", "Producto"])

        # Download whole historic series for all products from Yahoo! Finance
        df_stock = yf.download(self.products["ticker_yfinance"].to_list(),
                               df_tran["Fecha"].min())

        # Join the Quantity dataframe (with open positions) with the
        # stock data dataframe
        df_wide = df_qty.join(df_stock, how="left").fillna(method="ffill")

        # In case for some ticket yfinance doesn't return results
        tickets_missing = df_wide["Adj Close"].isna().any()
        for miss_ticket in tickets_missing[tickets_missing].index:
            # Download data for ticket from Alpha Vantage
            # using pandas_datareader
            alt_data = web.DataReader(miss_ticket, "av-daily",
                                      start=df_tran["Fecha"].min(),
                                      end=date.today(),
                                      api_key=av_api_key)
            # Convert index to DatetimeIndex
            alt_data.index = pd.to_datetime(alt_data.index)
            # Fill NaN with alternative data
            (df_wide.loc[:, ("Adj Close", miss_ticket)]
                    .fillna(alt_data["close"], inplace=True))
            # Fill weekends with ffill
            (df_wide.loc[:, ("Adj Close", miss_ticket)]
                    .fillna(method="ffill", inplace=True))

        # Create column with value of position
        # (df_wide[["Cantidad"]] * df_wide["Adj Close"]).rename(columns={"Cantidad": "Valor"})
        df_wide = df_wide.join(df_wide[["Cantidad"]].multiply(df_wide["Adj Close"])
                                                    .rename(columns={"Cantidad": "Valor"}))

        # Add column for Cash just in Value part
        df_wide["Valor", "Efectivo"] = (df_tran.drop_duplicates("Fecha", keep="last")
                                               .set_index("Fecha")["Efectivo"]
                                               .reindex(df_wide.index).fillna(method="ffill"))

        if format_out == "wide":
            return df_wide
        # Transform to "long" format (Fecha-Producto-Métrica index)
        elif format_out == "long":
            df_long = (df_wide.reset_index()
                              .melt(id_vars="Fecha")
                              .set_index(["Fecha", "Producto", "Métrica"]))
            return df_long
        # Transform to "mixed" format (Fecha-Producto index)
        elif format_out == "mixed":
            df_mixed = df_wide.stack(level="Producto", dropna=False)
            return df_mixed
        else:
            raise ValueError(f"{format_out} is not a valid format ('wide', 'long' or 'mixed').")

    def get_performance_df(self, df_tran: pd.DataFrame, df_pos: pd.DataFrame):
        """
        Returns an auxiliary `pandas.DataFrame` for computing performances
        (Time-Weighted Return and Money-Weighted Return)
        """

        # Create pandas.Series with movement info (Ingresos-Retiradas)
        s_mov = (df_tran.loc[df_tran["Tipo"].isin(["Ingreso", "Retirada"]),
                             ["Fecha", "Tipo", "Valor"]]
                        .groupby("Fecha")
                        .apply(lambda x:
                               x.loc[x["Tipo"] == "Ingreso", "Valor"].sum() -
                               x.loc[x["Tipo"] == "Retirada", "Valor"].sum())
                        .rename("movimientos"))
        # Create pandas.Series with current and previous value at same row level
        s_val_act = df_pos["Valor"].sum(axis=1).rename("valor act.")
        s_val_ant = df_pos["Valor"].sum(axis=1).shift().rename("valor ant.")

        # Create final pandas.DataFrame with daily Rate-Of-Return
        # (for computing TWR) ROR = (EV - (SV+CF)) / (SV+CF)
        # Here a deposit is positive CF, and withdrawal negative
        df_perf = pd.concat([s_val_act, s_val_ant, s_mov], axis=1).fillna(0.0)
        df_perf["ROR_daily"] = (
            (df_perf["valor act."] - (df_perf["valor ant."] + df_perf["movimientos"]))
            / (df_perf["valor ant."] + df_perf["movimientos"]) + 1)

        return df_perf


if __name__ == "__main__":
    notion = NotionAPI()
    df = notion.get_his_positions_df(notion.get_transactions_df())
    pprint(df)
