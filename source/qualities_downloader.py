from pathlib import Path
import json
import pandas as pd
import gspread
import logging
import logging.config

resolved_path = Path("../configs/logging_config.ini").resolve()
logging.config.fileConfig(resolved_path)


class QualitiesDownloader:
    """
    A class to download and process data from Google Sheets.

    This class provides functionality to download data from specified Google
    Sheets and process them into pandas DataFrames. It requires a config file
    to specify the details of the sheets and the Google Sheets client for API
    access.

    Attributes
    ----------
    sheets_name : str
        The key in the configuration file for accessing sheet names.
    self_key : str
        The key in the configuration file for self-related data.
    sheet_id_name : str
        The key for the sheet ID in the configuration.
    tab_index_name : str
        The key for the tab index in the configuration.
    rename_name : str
        The key for renaming columns in the configuration.
    reviewer_name : str
        The name used for the reviewer column in the dataframe.
    comment_name : str
        The name used for the comment column in the dataframe.
    config : dict
        The loaded configuration data.
    client : gspread.client.Client
        The Google Sheets client for API access.
    _self_dataframe : pd.DataFrame or None
        The dataframe containing self-related data.
    _others_dataframe : pd.DataFrame or None
        The dataframe containing data from others.

    Methods
    -------
    check_keys():
        Validates the presence of necessary keys in the configuration.
    download_sheet(sheet_key, tab_index=None):
        Downloads a specific sheet from Google Sheets and returns it as a
        pandas DataFrame.
    download_self():
        Downloads and processes the data related to 'self' from the
        configured sheet.
    refresh_self_dataframe():
        Refreshes the internal dataframe with self data.
    self_dataframe():
        Property that returns the current self dataframe.
    _download_other(rev_name, remove_unchosen=True):
        Downloads and processes data from a sheet specified by reviewer name.
    download_others(remove_unchosen=True):
        Downloads and processes data from sheets other than 'self'.
    refresh_others_dataframe():
        Refreshes the internal dataframe with others' data.
    others_dataframe():
        Property that returns the current dataframe with others' data.
    """

    sheets_name = "sheets"
    self_key = "self"
    sheet_id_name = "sheet_id"
    tab_index_name = "tab_index"
    rename_name = "rename"
    reviewer_name = "Name"
    comment_name = "Examples, so I can understand"

    def __init__(self, config_path: str, gs_client: gspread.client.Client):
        """
        Initializes the class from configuration file and Google Sheets client.

        Parameters
        ----------
        config_path : str
            The path to the JSON configuration file.
        gs_client : gspread.client.Client
            The Google Sheets client for accessing sheets.

        Raises
        ------
        FileNotFoundError
            If the configuration file cannot be found.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"Initializing Qualities with config file: {config_path}"
        )
        try:
            resolved_config_path = Path(config_path).resolve()
            with resolved_config_path.open(encoding="UTF-8") as file:
                self.config = json.load(file)
        except FileNotFoundError:
            self.logger.exception(f"Config file not found: {config_path}")
            raise
        self.client = gs_client
        self._self_dataframe = None
        self._others_dataframe = None
        self.logger.info(
            f"QualitiesDownloader initialised from ({str(Path(config_path))})."
        )

    def check_keys(self) -> None:
        """
        Checks if the necessary keys are present in the configuration.

        Validates the existence of essential keys in the configuration file
        necessary for the operation of the downloader.

        Raises
        ------
        KeyError
            If any required key is missing in the configuration.
        """
        necessary_keys = [self.sheets_name]
        missing = [key for key in necessary_keys if key not in self.config]
        if len(missing) > 0:
            error_str = ", ".join(
                [f"'{missing_key}'" for missing_key in missing]
            )
            raise KeyError(
                f"Keys {error_str} missing from config at {self.config}"
            )
        if self.self_key not in self.config.get(self.sheets_name):
            self_key = self.self_key
            base = f"'{self_key}' missing from config['{self.sheets_name}']."
            explanation = "Having it is prerequisite for evaluating feedback."
            raise KeyError(
                 f"{base} {explanation}"
            )
        sheets_w_missing_id_key = [
            name
            for name in self.config[self.sheets_name]
            if self.sheet_id_name not in self.config[self.sheets_name][name]
        ]
        if len(sheets_w_missing_id_key) > 0:
            sheet_id = self.sheet_id_name
            base = f"{sheet_id} is missing from {sheets_w_missing_id_key}."
            explanation = "It is necessary for downloading the data"
            raise KeyError(f"{base} {explanation}")
        self.logger.info("Keys in Qualities config checked.")

    def download_sheet(
            self,
            sheet_key: str,
            tab_index: int = None
    ) -> pd.DataFrame:
        """
        Downloads a specific sheet and returns it as a DataFrame.

        Parameters
        ----------
        sheet_key : str
            The unique key of the Google Sheet to download.
        tab_index : int, optional
            The index of the tab in the sheet to download (default is None,
            which implies the first tab).

        Returns
        -------
        pd.DataFrame
            The downloaded sheet data as a pandas DataFrame.

        Raises
        ------
        IndexError
            If the specified tab index is greater than the number of tabs
            in the sheet.
        """
        book = self.client.open_by_key(sheet_key)
        sheet_tab_index = 0
        if tab_index is not None:
            sheet_tab_index = tab_index
        try:
            worksheet = book.worksheets()[sheet_tab_index]
        except IndexError as e:
            tab_str = f"Tab index {sheet_tab_index}"
            tab_num = len(book.worksheets())
            sheet_str = f"length of tabs on {sheet_key}({tab_num})-1"
            self.logger.exception(
                f"{tab_str} is grater than {sheet_str}"
            )
            raise e
        table = worksheet.get_all_values()
        return pd.DataFrame(table[1:], columns=table[0])

    def download_self(self) -> pd.DataFrame:
        """
        Downloads the 'self' responses sheet based on the configuration.

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the data from the 'self' responses sheet.

        Notes
        -----
        This method uses the configuration details specified under 'self' key
        to determine which sheet and tab to download.
        """
        self_dict = self.config.get(self.sheets_name).get(self.self_key)
        sheet_key = self_dict.get(self.sheet_id_name)
        tab_index = self_dict.get(self.tab_index_name)
        column_renaming = self_dict.get(self.rename_name)
        tobereturned = self.download_sheet(
            sheet_key=sheet_key,
            tab_index=tab_index
        )
        if column_renaming is not None:
            return tobereturned.rename(columns=column_renaming)
        return tobereturned

    def refresh_self_dataframe(self) -> None:
        """
        Refreshes the internal DataFrame storing the 'self' responses.

        This method re-downloads the 'self' responses sheet and updates
        the internal DataFrame.
        """
        self._self_dataframe = self.download_self()
        sheet_name_str = (
            self.config
            .get(self.sheets_name)
            .get(self.self_key)
            .get(self.sheet_id_name)
        )
        message_str = "Data for own responses downloaded from"
        self.logger.info(f"{message_str} {sheet_name_str}.")

    @property
    def self_dataframe(self) -> pd.DataFrame:
        """
        Property that returns the current DataFrame of 'self' responses.

        This property ensures that the DataFrame is up-to-date by refreshing it
        if it hasn't been initialized yet.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing 'self' responses.
        """
        if self._self_dataframe is None:
            self.check_keys()
            self.refresh_self_dataframe()

        return self._self_dataframe

    def _download_other(
            self,
            rev_name: str,
            remove_unchosen: bool = True
    ) -> pd.DataFrame:
        """
        Processes the data from a sheet specified by a reviewer name.

        Parameters
        ----------
        rev_name : str
            The name of the reviewer whose responses are to be downloaded.
        remove_unchosen : bool, optional
            Flag to indicate whether to remove rows where the comment column
            is empty (default is True).

        Returns
        -------
        pd.DataFrame
            The DataFrame containing the responses from the specified
            reviewer's sheet.

        Raises
        ------
        KeyError
            If the comment column is not found in the sheet.
        """
        self.logger.debug(f"Sheet download for {rev_name} initiated.")
        other_dict = self.config.get(self.sheets_name).get(rev_name)
        sheet_key = other_dict.get(self.sheet_id_name)
        tab_index = other_dict.get(self.tab_index_name)
        column_renaming = other_dict.get(self.rename_name)
        tobereturned = self.download_sheet(
            sheet_key=sheet_key,
            tab_index=tab_index
        )
        if column_renaming is not None:
            tobereturned = tobereturned.rename(columns=column_renaming)
        tobereturned[self.reviewer_name] = str(rev_name).title()
        try:
            if remove_unchosen:
                tobereturned = tobereturned[
                    tobereturned[self.comment_name].str.len() > 0
                ].reset_index(drop=True).copy()
        except KeyError as e:
            if tab_index is None:
                tab_index = 0
            tab_str = f"on the {tab_index}th tab for {rev_name}"
            self.logger.exception(f"'{self.comment_name}' not found {tab_str}")
            raise e
        rev_num = tobereturned[
            tobereturned[self.comment_name].str.len() > 0
        ].shape[0]
        rev_name_t = str(rev_name).title()
        self.logger.info(f"{rev_name_t} has chosen {rev_num} adjectives.")

        return tobereturned

    def download_others(self, remove_unchosen: bool = True) -> pd.DataFrame:
        """
        Downloads and processes the data from sheets other than 'self'.

        Parameters
        ----------
        remove_unchosen : bool, optional
            Flag to indicate whether to remove rows where the comment column
            is empty (default is True).

        Returns
        -------
        pd.DataFrame
            A concatenated DataFrame containing the responses from all sheets
            except 'self'.
        """
        tobereturned = []
        for name in self.config.get(self.sheets_name):
            if not name == self.self_key:
                tobeappended = self._download_other(
                    name,
                    remove_unchosen=remove_unchosen
                )
                tobereturned.append(tobeappended)

        return pd.concat(tobereturned)

    def refresh_others_dataframe(self) -> None:
        """
        Refreshes the internal DataFrame storing others' responses.

        This method re-downloads the sheets for others' responses and updates
        the internal DataFrame.
        """
        self._others_dataframe = self.download_others()
        name_list = [
            name.title()
            for name in self.config.get(self.sheets_name)
            if not name == self.self_key
        ]
        self.logger.info(f"Other's responses downloaded for {name_list}.")

    @property
    def others_dataframe(self) -> pd.DataFrame:
        """
        Property that returns the current DataFrame of others' responses.

        This property ensures that the DataFrame is up-to-date by refreshing it
        if it hasn't been initialized yet.

        Returns
        -------
        pd.DataFrame
            The DataFrame containing others' responses.
        """
        if self._others_dataframe is None:
            self.check_keys()
            self.refresh_others_dataframe()

        return self._others_dataframe
