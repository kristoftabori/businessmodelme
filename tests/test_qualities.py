import pytest
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from ..source.qualities import GoogleSheetsClient


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