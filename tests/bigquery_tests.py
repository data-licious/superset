"""Unit tests for Superset"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from datetime import datetime
import json
import unittest

from mock import Mock, patch

from superset import db, sm, security
from superset.connectors.bigquery.models import BigQueryTable, BigQueryColumn, BigQueryMetric
from superset.connectors.bigquery.models import bigquery

from google.cloud.bigquery.dataset import Dataset
from .base_tests import SupersetTestCase

def _make_credentials():
    import google.auth.credentials

    return Mock(spec=google.auth.credentials.Credentials)

class BigQueryTests(SupersetTestCase):

    """Testing interactions with BigQuery"""

    def __init__(self, *args, **kwargs):
        super(BigQueryTests, self).__init__(*args, **kwargs)

    @patch('superset.connectors.bigquery.models.bigquery')
    def test_client(self, bigquery):
        self.login(username='admin')
        pass

if __name__ == '__main__':
    unittest.main()