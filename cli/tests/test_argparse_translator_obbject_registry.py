"""Test OBBject Registry."""

from unittest.mock import Mock

import pytest
from openbb_cli.argparse_translator.obbject_registry import Registry
from openbb_core.app.model.obbject import OBBject

# pylint: disable=redefined-outer-name, protected-access


@pytest.fixture
def registry():
    """Fixture to create a Registry instance for testing."""
    return Registry()


@pytest.fixture
def mock_obbject():
    """Fixture to create a mock OBBject for testing."""

    class MockModel:
        """Mock model for testing."""

        def __init__(self, value):
            self.mock_value = value
            self._model_json_schema = "mock_json_schema"

        def model_json_schema(self):
            return self._model_json_schema

    obb = Mock(spec=OBBject)
    obb.id = "123"
    obb.provider = "test_provider"
    obb.extra = {"command": "test_command"}
    obb._route = "/test/route"
    obb._standard_params = Mock()
    obb._standard_params = {}
    obb.results = [MockModel(1), MockModel(2)]
    return obb


def test_listing_all_obbjects(registry, mock_obbject):
    """Test listing all obbjects with additional properties."""
    registry.register(mock_obbject)

    all_obbjects = registry.all
    assert len(all_obbjects) == 1
    assert all_obbjects[0]["command"] == "test_command"
    assert all_obbjects[0]["provider"] == "test_provider"


def test_registry_initialization(registry):
    """Test the Registry is initialized correctly."""
    assert registry.obbjects == []


def test_register_new_obbject(registry, mock_obbject):
    """Test registering a new OBBject."""
    registry.register(mock_obbject)
    assert mock_obbject in registry.obbjects


def test_register_duplicate_obbject(registry, mock_obbject):
    """Test that duplicate OBBjects are not added."""
    registry.register(mock_obbject)
    registry.register(mock_obbject)
    assert len(registry.obbjects) == 1


def test_get_obbject_by_index(registry, mock_obbject):
    """Test retrieving an obbject by its index."""
    registry.register(mock_obbject)
    retrieved = registry.get(0)
    assert retrieved == mock_obbject


def test_remove_obbject_by_index(registry, mock_obbject):
    """Test removing an obbject by index."""
    registry.register(mock_obbject)
    registry.remove(0)
    assert mock_obbject not in registry.obbjects


def test_remove_last_obbject_by_default(registry, mock_obbject):
    """Test removing the last obbject by default."""
    registry.register(mock_obbject)
    registry.remove()
    assert not registry.obbjects


# ── Tests for _get_by_key ───────────────────────────────────────────


def test_get_by_key_found(registry, mock_obbject):
    """Test retrieving obbject by its register_key."""
    mock_obbject.extra = {"command": "test_command", "register_key": "my_data"}
    registry.register(mock_obbject)
    result = registry.get("my_data")
    assert result == mock_obbject


def test_get_by_key_not_found(registry, mock_obbject):
    """Test get returns None when key does not exist."""
    mock_obbject.extra = {"command": "test_command", "register_key": "other"}
    registry.register(mock_obbject)
    result = registry.get("missing")
    assert result is None


def test_get_by_key_no_register_key(registry, mock_obbject):
    """Test get by key when no register_key set."""
    mock_obbject.extra = {"command": "test_command"}
    registry.register(mock_obbject)
    result = registry.get("anything")
    assert result is None


# ── Tests for obbject_keys ──────────────────────────────────────────


def test_obbject_keys_empty(registry):
    """Test obbject_keys returns empty list when registry is empty."""
    assert registry.obbject_keys == []


def test_obbject_keys_with_keys(registry, mock_obbject):
    """Test obbject_keys returns register_keys."""
    mock_obbject.extra = {"command": "test", "register_key": "my_key"}
    registry.register(mock_obbject)
    assert registry.obbject_keys == ["my_key"]


def test_obbject_keys_skips_without_key(registry):
    """Test obbject_keys skips obbjects without register_key."""
    obj = Mock(spec=OBBject)
    obj.id = "456"
    obj.extra = {"command": "test"}
    obj.results = [{"data": 1}]
    registry.register(obj)
    assert registry.obbject_keys == []


# ── Tests for get with invalid type ─────────────────────────────────


def test_get_invalid_type_raises(registry):
    """Test get raises ValueError for invalid arg type."""
    with pytest.raises(ValueError, match="Couldn't get"):
        registry.get(3.14)


# ── Tests for all property edge cases ───────────────────────────────


def test_all_empty_registry(registry):
    """Test all returns empty dict for empty registry."""
    assert registry.all == {}


def test_all_index_as_stack_order(registry):
    """Test that all uses stack order (most recent = index 0)."""
    obj1 = Mock(spec=OBBject)
    obj1.id = "1"
    obj1.provider = "p1"
    obj1.extra = {"command": "cmd1"}
    obj1._route = "/r1"
    obj1._standard_params = {}
    obj1.results = [{"data": 1}]

    obj2 = Mock(spec=OBBject)
    obj2.id = "2"
    obj2.provider = "p2"
    obj2.extra = {"command": "cmd2"}
    obj2._route = "/r2"
    obj2._standard_params = {}
    obj2.results = [{"data": 2}]

    registry.register(obj1)
    registry.register(obj2)

    all_items = registry.all
    assert all_items[0]["command"] == "cmd2"  # most recent first
    assert all_items[1]["command"] == "cmd1"


def test_get_out_of_bounds(registry, mock_obbject):
    """Test get returns None for out-of-bounds index."""
    registry.register(mock_obbject)
    assert registry.get(99) is None
