import secrets
import requests
from typing import Union
from datetime import datetime
import pandas as pd
import yfinance as yf
from pprint import pprint

NOTION_URL = "https://api.notion.com/v1/databases/"
NOTION_VERSION = "2021-08-16"
DATABASE_ID = "aa1f83615ff945239302887264317370"

MARKETS_DICT = {"EAM": "AS", "XET": "DE"}


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

    def retrieve_db(self, token: str = secrets.INT_TOKEN,
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

    def query_db(self, token: str = secrets.INT_TOKEN,
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

        return df[["Fecha", "Tipo", "Producto", "ISIN", "Bolsa", "Centro ejecución",
                   "Símbolo", "Descripción", "Unidades", "Valor", "Tasa"]]

        """
        Returns `pandas.DataFrame` with whole historic of open positions

        Parameters
        ----------
        ```python
        df : pandas.DataFrame
        ```
            Dataframe containing list of transactions (obtained from `get_transactions_df()`)

        Returns
        -------
        ```python
        df_pos : pandas.DataFrame
        ```
            The tabulated result with dates from beginning to today as indexes and different \n
            financial products as columns.
        """

        # Generate date index
        date_idx = pd.date_range(df["Fecha"].min(), datetime.today().date(), name="Fecha")

        # Invert "Buy" units sign (for those that are recorded as positive)
        df.loc[(df["Tipo"] == "Venta") & (df["Unidades"] > 0), "Unidades"] *= -1

        # Pivot transactions dataframe to get as index the date_range and the products
        # as columns, propagating last valid observations
        df_pos = (df.loc[df["Tipo"].isin(["Compra", "Venta"])]
                    .pivot(index="Fecha", columns="Producto", values="Unidades")
                    .sort_index().cumsum().reindex(date_idx).fillna(method="ffill"))

        return df_pos

    def get_his_positions_df(self, df: pd.DataFrame, format_out: str = "wide",
                             markets_dict: dict = MARKETS_DICT) -> pd.DataFrame:
        """
        Returns `pandas.DataFrame` with whole historic of open positions

        Contains info about number of open positions, price, value and other\n
        stock info from *Yahoo! Finance* (module yfinance)

        Parameters
        ----------
        ```python
        df : pandas.DataFrame
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
        date_idx = pd.date_range(df["Fecha"].min(), datetime.today().date(), name="Fecha")

        # Invert "Buy" units sign (for those that are recorded as positive)
        df.loc[(df["Tipo"] == "Venta") & (df["Unidades"] > 0), "Unidades"] *= -1

        # Pivot transactions dataframe to get as index the date_range and the products
        # as columns, propagating last valid observations
        df_qty = (df.loc[df["Tipo"].isin(["Compra", "Venta"])]
                    .pivot(index="Fecha", columns="Producto", values="Unidades")
                    .sort_index().cumsum().reindex(date_idx).fillna(method="ffill"))

        # Get product info from transactions
        products = (df[["Producto", "ISIN", "Bolsa",
                        "Centro ejecución", "Símbolo"]].drop_duplicates()
                                                       .dropna())
        # Create ticker symbol compatible with yfinance
        products["ticker_yfinance"] = (products["Símbolo"] + "." +
                                       products["Bolsa"].replace(markets_dict))
        # Create MultiIndex with renamed columns to YFinance compatible tickets
        prod_col_level = df_qty.rename(columns=products.set_index("Producto")["ticker_yfinance"]
                                                       .to_dict()).columns
        df_qty.columns = pd.MultiIndex.from_product([["Cantidad"], prod_col_level],
                                                    names=["Métrica", "Producto"])

        # Download whole historic series for all products from Yahoo! Finance
        df_stock = yf.download(products["ticker_yfinance"].to_list(),
                               df["Fecha"].min())

        # Join the Quantity dataframe (with open positions) with the
        # stock data dataframe
        df_wide = df_qty.join(df_stock, how="left").fillna(method="ffill")
        # Create column with value of position
        # (df_wide[["Cantidad"]] * df_wide["Adj Close"]).rename(columns={"Cantidad": "Valor"})
        df_wide = df_wide.join(df_wide[["Cantidad"]].multiply(df_wide["Adj Close"])
                                                    .rename(columns={"Cantidad": "Valor"}))

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


if __name__ == "__main__":
    notion = NotionAPI()
    df = notion.get_his_positions_df(notion.get_transactions_df())
    pprint(df)
