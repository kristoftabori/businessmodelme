from pathlib import Path
import json

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import logging
import logging.config
resolved_path = Path("../configs/logging_config.ini").resolve()
logging.config.fileConfig(resolved_path)


class GoogleSheetsClient:
    """
    A client class to interact with Google Sheets using the gspread library.

    This class is responsible for setting up a connection to Google Sheets
    with the provided configuration. It ensures necessary configuration keys
    are present and creates a client for Google Sheets interactions.

    Attributes
    ----------
    credentials_name : str
        Name of the key in the config for Google service account credentials.
    scope_name : str
        Name of the key in the config for the scope of the Google service
        account.
    _client : gspread.client.Client, optional
        The gspread client to interact with Google Sheets (default is None).

    Methods
    -------
    check_keys():
        Checks if necessary keys for configuration are present.
    client():
        Property that returns a gspread client to interact with Google Sheets.
    """

    credentials_name = "credentials"
    scope_name = "scope"

    def __init__(self, config_path: str):
        """
        Initializes the GoogleSheetsClient with a specified configuration file.

        Parameters
        ----------
        config_path : str
            Path to the configuration file in JSON format, containing Google
            service account credentials and scope.

        Raises
        ------
        FileNotFoundError
            If the specified configuration file is not found.
        KeyError
            If required keys are missing in the configuration file.
        """
        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            f"Initializing GoogleSheetsClient with config file: {config_path}"
        )
        try:
            resolved_config_path = Path(config_path).resolve()
            with resolved_config_path.open(encoding="UTF-8") as file:
                self.config = json.load(file)
        except FileNotFoundError:
            self.logger.exception(
                f"Configuration file not found: {config_path}"
            )
            raise
        self._client = None

        self.logger.info(
            f"GoogleSheetsClient from {str(resolved_config_path)} initialized."
        )

    def check_keys(self):
        """
        Checks if the necessary configuration keys are present.

        This method validates the presence of 'credentials' and 'scope' keys
        in the configuration. It also checks for the presence of 'project_id'
        within the 'credentials' key.

        Raises
        ------
        KeyError
            If any of the required keys are missing in the configuration.
        """
        necessary_keys = [self.credentials_name, self.scope_name]
        missing = [key for key in necessary_keys if key not in self.config]
        if len(missing) > 0:
            error_str = ", ".join(
                [f"'{missing_key}'" for missing_key in missing]
            )
            raise KeyError(
                f"Keys {error_str} missing from config at {self.config}"
            )
        if "project_id" not in self.config.get("credentials"):
            raise KeyError(
                f"'project_id' missing from config['{self.credentials_name}']"
            )
        self.logger.info("Keys in GoogleSheetsClient config checked.")

    @property
    def client(self) -> gspread.client.Client:
        """
        Provides the gspread client for Google Sheets interaction.

        This is a property method that checks if the client is already
        initialized. If not, it initializes the client using the service
        account credentials and scope from the configuration. It ensures
        that all necessary keys are checked before client initialization.

        Returns
        -------
        gspread.client.Client
            The initialized client for interacting with Google Sheets.

        Raises
        ------
        KeyError
            If required keys are missing in the configuration for client
            initialization.
        """
        if self._client is None:
            self.check_keys()
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                self.config.get(self.credentials_name),
                self.config.get(self.scope_name)
            )
            self._client = gspread.authorize(credentials)
            proj_id = self.config.get(self.credentials_name).get('project_id')
            self.logger.info(
                f"GoogleSheetsClient ({proj_id}) connection established."
            )
        return self._client
