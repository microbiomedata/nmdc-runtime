"""
Tests for multivalued was_informed_by functionality.

This module tests that the runtime can handle both single-valued and 
multivalued was_informed_by fields, ensuring backward compatibility.
"""

import pytest
from nmdc_runtime.api.core.util_multivalued import (
    normalize_to_list,
    get_was_informed_by_values,
    create_was_informed_by_query,
    create_was_informed_by_reverse_query,
)


class TestMultivaluedUtils:
    """Test utility functions for handling multivalued was_informed_by."""

    def test_normalize_to_list_single_value(self):
        """Test normalizing a single string value to list."""
        result = normalize_to_list("single_value")
        assert result == ["single_value"]

    def test_normalize_to_list_already_list(self):
        """Test normalizing a list that's already a list."""
        result = normalize_to_list(["value1", "value2"])
        assert result == ["value1", "value2"]

    def test_normalize_to_list_none(self):
        """Test normalizing None to empty list."""
        result = normalize_to_list(None)
        assert result == []

    def test_normalize_to_list_empty_list(self):
        """Test normalizing empty list."""
        result = normalize_to_list([])
        assert result == []

    def test_get_was_informed_by_values_single(self):
        """Test extracting single was_informed_by value from document."""
        doc = {"was_informed_by": "nmdc:dgns-00-000001"}
        result = get_was_informed_by_values(doc)
        assert result == ["nmdc:dgns-00-000001"]

    def test_get_was_informed_by_values_multivalued(self):
        """Test extracting multivalued was_informed_by from document."""
        doc = {"was_informed_by": ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]}
        result = get_was_informed_by_values(doc)
        assert result == ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]

    def test_get_was_informed_by_values_missing(self):
        """Test extracting was_informed_by from document without the field."""
        doc = {"id": "nmdc:wfmgan-00-000001"}
        result = get_was_informed_by_values(doc)
        assert result == []

    def test_create_was_informed_by_query_single(self):
        """Test creating query for single was_informed_by value."""
        result = create_was_informed_by_query("nmdc:dgns-00-000001")
        expected = {"was_informed_by": "nmdc:dgns-00-000001"}
        assert result == expected

    def test_create_was_informed_by_query_multivalued(self):
        """Test creating query for multivalued was_informed_by."""
        result = create_was_informed_by_query(["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"])
        expected = {"was_informed_by": {"$in": ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]}}
        assert result == expected

    def test_create_was_informed_by_query_none(self):
        """Test creating query for None was_informed_by."""
        result = create_was_informed_by_query(None)
        assert result == {}

    def test_create_was_informed_by_query_empty_list(self):
        """Test creating query for empty was_informed_by list."""
        result = create_was_informed_by_query([])
        assert result == {}

    def test_create_was_informed_by_reverse_query(self):
        """Test creating reverse query to find documents with specific was_informed_by value."""
        result = create_was_informed_by_reverse_query("nmdc:dgns-00-000001")
        expected = {"was_informed_by": "nmdc:dgns-00-000001"}
        assert result == expected


class TestBackwardCompatibility:
    """Test that the changes maintain backward compatibility."""

    def test_single_valued_workflow_execution(self):
        """Test processing workflow execution with single was_informed_by value."""
        workflow_execution = {
            "id": "nmdc:wfmgan-00-000001.1",
            "was_informed_by": "nmdc:dgns-00-000001",
            "has_input": ["nmdc:bsm-00-000001"],
            "has_output": ["nmdc:dobj-00-000001"]
        }
        
        was_informed_by_values = get_was_informed_by_values(workflow_execution)
        assert was_informed_by_values == ["nmdc:dgns-00-000001"]
        
        # Test that we can iterate over it
        for value in was_informed_by_values:
            assert value == "nmdc:dgns-00-000001"

    def test_multivalued_workflow_execution(self):
        """Test processing workflow execution with multivalued was_informed_by."""
        workflow_execution = {
            "id": "nmdc:wfmgan-00-000001.1", 
            "was_informed_by": ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"],
            "has_input": ["nmdc:bsm-00-000001"],
            "has_output": ["nmdc:dobj-00-000001"]
        }
        
        was_informed_by_values = get_was_informed_by_values(workflow_execution)
        assert was_informed_by_values == ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]
        
        # Test that we can iterate over both values
        expected_values = ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]
        for i, value in enumerate(was_informed_by_values):
            assert value == expected_values[i]


# Integration test style for faker
def test_faker_backward_compatibility():
    """Test that faker still works with single values."""
    from tests.lib.faker import Faker
    
    f = Faker()
    # Test original single-valued usage still works
    annotations = f.generate_metagenome_annotations(
        1, 
        was_informed_by="nmdc:dgns-00-000001", 
        has_input=["nmdc:bsm-00-000001"]
    )
    
    assert len(annotations) == 1
    assert annotations[0]["was_informed_by"] == "nmdc:dgns-00-000001"


def test_faker_multivalued_support():
    """Test that faker now supports multivalued was_informed_by."""
    from tests.lib.faker import Faker
    
    f = Faker()
    # Test new multivalued usage
    annotations = f.generate_metagenome_annotations(
        1,
        was_informed_by=["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"],
        has_input=["nmdc:bsm-00-000001"]
    )
    
    assert len(annotations) == 1
    assert annotations[0]["was_informed_by"] == ["nmdc:dgns-00-000001", "nmdc:dgns-00-000002"]