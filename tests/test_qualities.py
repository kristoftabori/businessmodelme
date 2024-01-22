import pytest
import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from unittest.mock import Mock, patch
from ..source.qualities_downloader import QualitiesDownloader
from ..source.qualities_summary import FeedbackSummary
from ..source.google_sheets_client import GoogleSheetsClient


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

        
class TestFeedbackSummary:

    def setup_method(self, method):
        self.my_df = pd.DataFrame([
            {
                "Quality": "Abstract thinker",
                "Comment": "I like abstract base classes",
            },
            {
                "Quality": "Analytical",
                "Comment": "I like to analyse code",
            },
            {
                "Quality": "Spontaneous",
                "Comment": "I like to jump on exciting projects",
            },
            {
                "Quality": "Committed",
                "Comment": "if I commit myself to a project, I will push forward until it's done",
            },
            {
                "Quality": "Independent",
                "Comment": "",
            },
            {
                "Quality": "Ambitious",
                "Comment": "",
            },
        ])
        self.others_df = pd.DataFrame([
            {
                "Quality": "Abstract thinker",
                "Examples, so I can understand": "you like to perform induction and deduction",
                "Name": "A"
            },
            {
                "Quality": "Abstract thinker",
                "Examples, so I can understand": "you often speak in abstract terms",
                "Name": "B"
            },
            {
                "Quality": "Analytical",
                "Examples, so I can understand": "you break down problems to smaller pieces very well and often",
                "Name": "A"
            },
            {
                "Quality": "Ambitious",
                "Examples, so I can understand": "you always want to build the best tool",
                "Name": "B"
            },
            {
                "Quality": "Committed",
                "Examples, so I can understand": "I've never seen you drop off a project",
                "Name": "B"
            },
            {
                "Quality": "Independent",
                "Examples, so I can understand": "you don't require others to start something",
                "Name": "A"
            },
            {
                "Quality": "Independent",
                "Examples, so I can understand": "you kicked out the other project on your own and it was a blast",
                "Name": "B"
            },
        ])
        self.hierarchy = hierarchy = ["Quality", "Others Count",  "My Examples","Name", "Their Examples"]
        
    def test_init(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        assert my_summary.self_dataframe is not None
        assert my_summary.others_dataframe is not None
        assert my_summary.hierarchy is not None

    def test_check_qualities(self):
        extra_line = [{
            "Quality": "Independent",
            "Comment": "",
        }]
        two_extra_lines = [
            {
                "Quality": "Independent",
                "Comment": "",
            },
            {
                "Quality": "Committed",
                "Comment": "if I commit myself to a project, I will psuh forward until ti's done",
            },
        ]
        with pytest.raises(ValueError) as e:
            my_other_summary = FeedbackSummary(pd.concat([self.my_df, pd.DataFrame(extra_line)]), self.others_df, self.hierarchy)
        
        assert "There is a duplicated entry found" in str(e.value)
        with pytest.raises(ValueError) as e:
            my_other_summary = FeedbackSummary(pd.concat([self.my_df, pd.DataFrame(two_extra_lines)]), self.others_df, self.hierarchy)
        
        assert "There are duplicated entries found" in str(e.value)
        with pytest.raises(ValueError) as e:
            my_other_summary = FeedbackSummary(self.my_df.iloc[:-1], self.others_df, self.hierarchy)
        
        assert "There is a quality missing from" in str(e.value)
        with pytest.raises(ValueError) as e:
            my_other_summary = FeedbackSummary(self.my_df.iloc[1:-1], self.others_df, self.hierarchy)
        
        assert "There are qualities missing from" in str(e.value)

    def test_count_dataframe_creation(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        assert my_summary._count_dataframe is None
        assert isinstance(my_summary.count_dataframe, pd.DataFrame)
        assert "Others Count" in my_summary.count_dataframe.columns
        assert my_summary.count_dataframe.loc[my_summary.count_dataframe["Quality"] == "Abstract thinker", "Others Count"].reset_index(drop=True)[0] == 2
        assert my_summary.count_dataframe.loc[my_summary.count_dataframe["Quality"] == "Analytical", "Others Count"].reset_index(drop=True)[0] == 1

    def test_merged_dataframe_creation(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        assert my_summary._merged_dataframe is None
        assert isinstance(my_summary.merged_dataframe, pd.DataFrame)
        assert not any(my_summary.merged_dataframe.isna().any())
        assert my_summary.merged_dataframe["Others Count"].is_monotonic_decreasing
        assert my_summary.merged_dataframe.loc[my_summary.merged_dataframe["Others Count"] == 2, "Quality"].is_monotonic_increasing
        assert my_summary.merged_dataframe.loc[my_summary.merged_dataframe["Others Count"] == 1, "Quality"].is_monotonic_increasing
        assert my_summary.merged_dataframe.loc[my_summary.merged_dataframe["Others Count"] == 0, "Quality"].is_monotonic_increasing
        for quality in my_summary.merged_dataframe["Quality"].unique():
            assert my_summary.merged_dataframe.loc[my_summary.merged_dataframe["Quality"] == quality, "Name"].is_monotonic_increasing

    def test_match_df(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        match_df = my_summary.match_dataframe()
        assert "Abstract thinker" in list(match_df["Quality"])
        for quality in list(match_df["Quality"].unique()):
            count_list = list(match_df.loc[match_df["Quality"] == quality, "Others Count"])
            assert all([element == len(count_list) for element in count_list])

    def test_only_me_df(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        only_me_df = my_summary.only_me_dataframe()
        assert "Spontaneous" in list(only_me_df["Quality"])
        assert only_me_df.shape[0] == 1
        assert all([element == 0 for element in list(only_me_df["Others Count"])])

    def test_only_others_df(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        only_others_df = my_summary.only_others_dataframe()
        assert "Independent" in list(only_others_df["Quality"])
        assert "Ambitious" in list(only_others_df["Quality"])
        assert only_others_df.shape[0] == 3
        assert all([element > 0 for element in list(only_others_df["Others Count"])])
        for quality in list(only_others_df["Quality"].unique()):
            count_list = list(only_others_df.loc[only_others_df["Quality"] == quality, "Others Count"])
            assert all([element == len(count_list) for element in count_list])

    def test_remove_redundancies(self):
        my_summary = FeedbackSummary(self.my_df, self.others_df, self.hierarchy)
        removed_df = my_summary.remove_redundancies(my_summary.match_dataframe())
        qualities = [quality for quality in list(my_summary.match_dataframe()["Quality"].unique())]
        assert all([removed_df.loc[removed_df["Quality"] == quality, "Quality"].shape[0] == 1 for quality in qualities])

        



