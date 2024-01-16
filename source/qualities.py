from pathlib import Path
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

class GoogleSheetsClient:

    credentials_name = "credentials"
    scope_name = "scope"

    def __init__(self, config_path: str):
        logging.debug(f"Initializing GoogleSheetsClient with config file: {config_path}")
        try:
            self.config = json.loads(Path(config_path).resolve().read_text(encoding="UTF-8"))
        except FileNotFoundError as e:
            logging.error(f"Configuration file not found: {config_path}")
            raise e
        self._client = None

        logging.info(f"GoogleSheetsClient from {str(Path(config_path).resolve())} initialized.")

    def check_keys(self):
        necessary_keys = [self.credentials_name, self.scope_name]
        missing = [key for key in necessary_keys if key not in self.config]
        if len(missing) > 0:
            error_str = ", ".join([f"'{missing_key}'" for missing_key in missing])
            raise KeyError(f"Keys {error_str} missing from config at {self.config}")
        if "project_id" not in self.config.get("credentials"):
            raise KeyError(f"'project_id' missing from config['{self.credentials_name}']")
        
        logging.info("Keys in GoogleSheetsClient config checked.")

    @property
    def client(self) -> gspread.client.Client:
        if self._client is None:
            self.check_keys()
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.config.get("credentials"), self.config.get("scope"))
            self._client = gspread.authorize(credentials)

            logging.info(f"GoogleSheetsClient ({self.config.get('credentials').get('project_id')}) connection established.")
        return self._client


class QualitiesDownloader:

    sheets_name = "sheets"
    self_key = "self"
    sheet_id_name = "sheet_id"
    tab_index_name = "tab_index"
    rename_name = "rename"
    reviewer_name = "Name"
    comment_name = "Examples, so I can understand"

    def __init__(self, config_path: str, gs_client: gspread.client.Client):
        logging.debug(f"Initializing Qualities with config file: {config_path}")
        try:
            self.config = json.loads(Path(config_path).resolve().read_text(encoding="UTF-8"))
        except FileNotFoundError as e:
            logging.error(f"Configuration file not found: {config_path}")
            raise e
        self.client = gs_client
        self._self_dataframe = None
        self._others_dataframe = None
        
    def check_keys(self):
        necessary_keys = [self.sheets_name]
        missing = [key for key in necessary_keys if key not in self.config]
        if len(missing) > 0:
            error_str = ", ".join([f"'{missing_key}'" for missing_key in missing])
            raise KeyError(f"Keys {error_str} missing from config at {self.config}")
        if self.self_key not in self.config.get(self.sheets_name):
            raise KeyError(f"'{self.self_key}' missing from config['{self.sheets_name}']. Having it is prerequisite for evaluating feedback.")
        sheets_w_missing_id_key = [name for name in self.config[self.sheets_name] if self.sheet_id_name not in self.config[self.sheets_name][name]]
        if len(sheets_w_missing_id_key) > 0:
            raise KeyError(f"{self.sheet_id_name} is missing from {sheets_w_missing_id_key}. It is necessary for downloading the data")
        
        logging.info("Keys in Qualities config checked.")

    def download_sheet(self, sheet_key: str, tab_index: int = None) -> pd.DataFrame:
        book = self.client.open_by_key(sheet_key)
        sheet_tab_index = 0
        if tab_index is not None:
            sheet_tab_index = tab_index
        try:
            worksheet = book.worksheets()[sheet_tab_index]
        except IndexError as e:
            logging.error(f"Tab index {sheet_tab_index} is grater than length of tabs on {sheet_key}({len(book.worksheets())})-1")
            raise e
        table = worksheet.get_all_values()
        return pd.DataFrame(table[1:], columns=table[0])

    def download_self(self) -> pd.DataFrame:
        self_dict = self.config.get(self.sheets_name).get(self.self_key)
        sheet_key = self_dict.get(self.sheet_id_name)
        tab_index = self_dict.get(self.tab_index_name)
        column_renaming = self_dict.get(self.rename_name)
        tobereturned = self.download_sheet(sheet_key=sheet_key, tab_index=tab_index)
        if column_renaming is not None:
            return tobereturned.rename(columns=column_renaming)
        return tobereturned
    
    def refresh_self_dataframe(self):
        self._self_dataframe = self.download_self()
        logging.info(f"Data for own responses downloaded from {self.config.get(self.sheets_name).get(self.self_key).get(self.sheet_id_name)}.")

    @property
    def self_dataframe(self):
        if self._self_dataframe is None:
            self.check_keys()
            self.refresh_self_dataframe()

        return self._self_dataframe

    def download_others(self, remove_unchosen: bool = True) -> pd.DataFrame:
        tobereturned = []
        for name in [name for name in self.config.get(self.sheets_name) if not name == self.self_key]:
            other_dict = self.config.get(self.sheets_name).get(name)
            sheet_key = other_dict.get(self.sheet_id_name)
            tab_index = other_dict.get(self.tab_index_name)
            column_renaming = other_dict.get(self.rename_name)
            tobeappended = self.download_sheet(sheet_key=sheet_key, tab_index=tab_index)
            if column_renaming is not None:
                tobeappended = tobeappended.rename(columns=column_renaming)
            tobeappended[self.reviewer_name] = str(name).title()
            if remove_unchosen:
                tobeappended = tobeappended[tobeappended[self.comment_name].str.len()>0].reset_index(drop=True).copy()
            logging.info(f"{str(name).title()} has chosen {tobeappended[tobeappended[self.comment_name].str.len()>0].shape[0]} adjectives.")
            tobereturned.append(tobeappended)

        return pd.concat(tobereturned)
    
    def refresh_others_dataframe(self):
        self._others_dataframe = self.download_others()
        logging.info(f"Other's responses downloaded for {[name.title() for name in self.config.get(self.sheets_name) if not name == self.self_key]}.")

    @property
    def others_dataframe(self) -> pd.DataFrame:
        if self._others_dataframe is None:
            self.check_keys()
            self.refresh_others_dataframe()

        return self._others_dataframe
        

def load_sheet(name: str, sheets_data, connection, add_name: bool = True) -> pd.DataFrame:
    id_data = sheets_data.get(name)
    spreadsheet_key = id_data.get("sheet_id")
    book = connection.open_by_key(spreadsheet_key)
    worksheet = book.worksheet(id_data.get("sheet_name"))
    table = worksheet.get_all_values()
    sheet_df = pd.DataFrame(table[1:], columns=table[0])
    if add_name:
        sheet_df["Name"] = name.title()
    return sheet_df