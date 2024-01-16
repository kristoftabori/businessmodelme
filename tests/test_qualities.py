import pytest
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from unittest.mock import Mock, patch
from ..source.qualities import GoogleSheetsClient, QualitiesDownloader


class TestGoogleSheetsClient:
    @classmethod
    @pytest.fixture
    def valid_google_client_config(cls, tmp_path):
        config = {
            'credentials': {'type': 'service_account', 'project_id': 'test_project'},
            'scope': ['https://spreadsheets.google.com/feeds']
        }
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(config))
        return str(config_path)

    def test_init_with_valid_config(self, valid_google_client_config):
        client = GoogleSheetsClient(valid_google_client_config)
        assert client.config is not None, "The client should have a config"

    @staticmethod
    def test_init_with_invalid_config():
        with pytest.raises(FileNotFoundError):
            GoogleSheetsClient("non_existing_path.json")

    def test_check_keys_with_missing_credentials(self, valid_google_client_config):
        with pytest.raises(KeyError) as excinfo:
            client = GoogleSheetsClient(valid_google_client_config)
            del client.config['credentials']
            client.check_keys()
        assert "'credentials' missing from config" in str(excinfo.value)

    def test_check_keys_with_missing_project_id(self, valid_google_client_config):
        with pytest.raises(KeyError) as excinfo:
            client = GoogleSheetsClient(valid_google_client_config)
            del client.config['credentials']['project_id']
            client.check_keys()
        assert "'project_id' missing from config['credentials']" in str(excinfo.value)

    def test_client_property_with_valid_config(self, valid_google_client_config, monkeypatch):
        # Mock the gspread and oauth2client to avoid actual API calls
        monkeypatch.setattr(gspread, 'authorize', lambda creds: 'MockedClient')
        monkeypatch.setattr(ServiceAccountCredentials, 'from_json_keyfile_dict', lambda config, scope: 'MockedCredentials')

        client = GoogleSheetsClient(valid_google_client_config)
        assert client.client == 'MockedClient', "Client should be initialized"


@pytest.fixture
def mock_gs_client():
    # Mock the gspread client
    client = Mock(spec=gspread.client.Client)
    return client

@pytest.fixture
def valid_config():
    return {
        "sheets": {
            "self": {
                "sheet_id": "test_sheet_id_self",
                "tab_index": 0,
                "rename": {"old_name": "new_name"}
            },
            "other1": {
                "sheet_id": "test_sheet_id_other_1",
                "tab_index": 1,
                "rename": {"old_name": "Examples, so I can understand"}
            },
            "other2": {
                "sheet_id": "test_sheet_id_other_2",
                "rename": {"old_name": "Examples, so I can understand"}
            }
        }
    }


class TestQualitiesDownloader:
    
    def test_init_qualities_downloader(self, mock_gs_client, valid_config, tmp_path):
        # Write valid config to a temporary file
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))

        # Test initialization
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)
        assert downloader.client is not None

    def test_check_keys_missing_sheets(self, mock_gs_client, valid_config, tmp_path):
        # Modify valid_config to remove necessary keys and test
        del valid_config['sheets']
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))

        downloader = QualitiesDownloader(str(config_path), mock_gs_client)
        with pytest.raises(KeyError):
            downloader.check_keys()

    def test_download_sheet(self, mock_gs_client, valid_config, tmp_path):
        # Mock the chain of method calls
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [['Header1', 'Header2'], ['Value1', 'Value2']]
        mock_gs_client.open_by_key.return_value.worksheets.return_value = [mock_worksheet]

        # Setup the rest of the test
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)

        df = downloader.download_sheet('test_sheet_id_self')
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_download_self(self, mock_gs_client, valid_config, tmp_path):
        # Mock the chain of method calls
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [['old_name'], ['Value1']]
        mock_gs_client.open_by_key.return_value.worksheets.return_value = [mock_worksheet]

        # Setup the test
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)

        df = downloader.download_self()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert "new_name" in df.columns  # Check if columns are correctly renamed
        assert "old_name" not in df.columns  # Check if columns are correctly renamed

    def test_refresh_self_dataframe(self, mock_gs_client, valid_config, tmp_path):
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [['old_name'], ['Value1']]
        mock_gs_client.open_by_key.return_value.worksheets.return_value = [mock_worksheet]

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)

        downloader.refresh_self_dataframe()
        assert downloader._self_dataframe is not None
        assert isinstance(downloader._self_dataframe, pd.DataFrame)

    def test_self_dataframe(self, mock_gs_client, valid_config, tmp_path):
        mock_worksheet = Mock()
        mock_worksheet.get_all_values.return_value = [['old_name'], ['Value1']]
        mock_gs_client.open_by_key.return_value.worksheets.return_value = [mock_worksheet]

        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)

        df = downloader.self_dataframe
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_download_others(self, mock_gs_client, valid_config, tmp_path):
        # Create mock worksheets with different return values
        mock_worksheet_self = Mock()
        mock_worksheet_self.get_all_values.return_value = [['old_name'], ['Value1']]
        
        mock_worksheet_other_1 = Mock()
        mock_worksheet_other_1.get_all_values.return_value = [['old_name'], ['Value2']]
        
        mock_worksheet_other_2 = Mock()
        mock_worksheet_other_2.get_all_values.return_value = [['old_name'], ['Value3']]

        # Create mock workbooks and set up their 'worksheets' method
        mock_workbook_self = Mock()
        mock_workbook_self.worksheets.return_value = [mock_worksheet_self]

        mock_workbook_other = Mock()
        mock_workbook_other.worksheets.return_value = [mock_worksheet_other_1, mock_worksheet_other_2]

        # Ensure 'open_by_key' method returns the correct mock workbook
        def open_by_key_side_effect(sheet_key):
            if sheet_key == "test_sheet_id_self":
                return mock_workbook_self
            elif sheet_key == "test_sheet_id_other_1":
                return mock_workbook_other
            elif sheet_key == "test_sheet_id_other_2":
                return mock_workbook_other
            else:
                raise ValueError("Invalid sheet key")

        mock_gs_client.open_by_key.side_effect = open_by_key_side_effect

        # Setup the rest of the test
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(valid_config))
        downloader = QualitiesDownloader(str(config_path), mock_gs_client)

        df = downloader.download_others()
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert len(df) == 2  # Assuming one row per sheet

        



