"""Test mapped_streams functionality."""

import json
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from target_s3.formats.format_base import FormatBase


class TestMappedStreams:
    """Test cases for mapped_streams functionality."""

    def test_mapped_streams_config_parsing(self):
        """Test that mapped_streams configuration is parsed correctly."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "mapped_streams": '{"madkudu_events_table": "event", "madkudu_contacts_table": "contact_update"}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "madkudu_events_table",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            assert format_base.mapped_streams == {
                "madkudu_events_table": "event",
                "madkudu_contacts_table": "contact_update"
            }

    def test_mapped_streams_invalid_json(self):
        """Test that invalid JSON in mapped_streams is handled gracefully."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "mapped_streams": '{"invalid": json}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "test_stream",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            # Should default to empty dict on invalid JSON
            assert format_base.mapped_streams == {}

    def test_create_key_with_mapped_stream(self):
        """Test that create_key uses mapped stream name when available."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "prefix": "test-prefix",
            "mapped_streams": '{"madkudu_events_table": "event"}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "madkudu_events_table",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            # The key should use "event" instead of "madkudu_events_table"
            key = format_base.create_key()
            assert "event" in key
            assert "madkudu_events_table" not in key

    def test_create_key_without_mapped_stream(self):
        """Test that create_key uses original stream name when no mapping exists."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "prefix": "test-prefix",
            "mapped_streams": '{"madkudu_events_table": "event"}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "other_stream",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            # The key should use "other_stream" since it's not in the mapping
            key = format_base.create_key()
            assert "other_stream" in key
            assert "event" not in key

    def test_create_key_priority_order(self):
        """Test that mapped_streams takes priority over stream_name_path_override."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "prefix": "test-prefix",
            "stream_name_path_override": "override_name",
            "mapped_streams": '{"madkudu_events_table": "event"}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "madkudu_events_table",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            # The key should use "event" (from mapped_streams) instead of "override_name"
            key = format_base.create_key()
            assert "event" in key
            assert "override_name" not in key

    def test_create_key_with_tenant_and_mapped_streams(self):
        """Test that tenant prefix works correctly with mapped_streams."""
        config = {
            "format": {"format_type": "json"},
            "cloud_provider": {
                "cloud_provider_type": "aws",
                "aws": {
                    "aws_bucket": "test-bucket",
                    "aws_region": "us-west-2"
                }
            },
            "prefix": "test-prefix",
            "tenant": "acme_corp",
            "mapped_streams": '{"madkudu_events_table": "event"}',
            "append_date_to_prefix": False,
            "append_date_to_filename": False
        }
        
        context = {
            "stream_name": "madkudu_events_table",
            "batch_start_time": datetime.now(),
            "logger": Mock()
        }
        
        # Mock the abstract methods
        with patch.multiple(FormatBase, __abstractmethods__=set()):
            format_base = FormatBase(config, context, "json")
            
            # The key should include tenant prefix + mapped stream name
            key = format_base.create_key()
            assert "acme_corp_event" in key  # tenant prefix + mapped stream name
            assert "madkudu_events_table" not in key
            assert "acme_corp_madkudu_events_table" not in key 