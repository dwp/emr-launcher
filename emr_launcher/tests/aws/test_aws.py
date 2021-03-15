import pytest
from unittest.mock import patch


class TestConfig:
    @patch("emr_launcher.aws._get_client")
    def test_emr_cluster_add_tags(self, mock_get_client):
        pass
