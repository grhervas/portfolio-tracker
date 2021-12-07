import secrets
import requests
from typing import Union
from datetime import datetime
import pandas as pd
from pprint import pprint

NOTION_URL = "https://api.notion.com/v1/databases/"
NOTION_VERSION = "2021-08-16"
DATABASE_ID = "aa1f83615ff945239302887264317370"


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

    def get_transactions_dataframe(self) -> pd.DataFrame:
        """
        Returns `pandas.DataFrame` from Notion's database.

        Iterates through results from CURL response (following json schema)
        and applies appropiate format to each field.

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


if __name__ == "__main__":
    notion = NotionAPI()
    df = notion.get_transactions_dataframe()
    pprint(df)
