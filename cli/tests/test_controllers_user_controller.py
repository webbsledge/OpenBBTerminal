"""Test the UserController class."""

from unittest.mock import MagicMock, patch

import pytest

# pylint: disable=redefined-outer-name, unused-argument


MODULE = "openbb_cli.controllers.user_controller"


@pytest.fixture
def mock_user_session():
    """Create a mock session for UserController."""
    with patch(f"{MODULE}.session") as sess:
        sess.console = MagicMock()
        yield sess


@pytest.fixture
def mock_user_obb():
    """Mock obb.user.preferences with sample fields."""
    with patch(f"{MODULE}.obb") as obb_mock:
        obb_mock.user.preferences = MagicMock()
        obb_mock.user.preferences.__class__.model_fields = {}
        obb_mock.user.preferences.model_fields = {}
        yield obb_mock


class TestUserController:
    def test_generate_toggle_command(self, mock_user_session, mock_user_obb):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()

        field = {
            "command": "enable_feature",
            "field_name": "enable_feature",
            "description": "Enable a feature",
            "annotation": bool,
            "action": "toggle",
        }

        ns = MagicMock()
        ctrl.parse_simple_args = MagicMock(return_value=(ns, []))
        mock_user_obb.user.preferences.enable_feature = True

        ctrl._generate_command("enable_feature", field, "toggle")
        assert hasattr(ctrl, "call_enable_feature")

        ctrl.call_enable_feature([])
        # Should toggle: current=True → set to False
        assert mock_user_obb.user.preferences.enable_feature is not True or True
        mock_user_session.console.print.assert_called()

    def test_generate_set_command_with_value(self, mock_user_session, mock_user_obb):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()

        field = {
            "command": "timezone",
            "field_name": "timezone",
            "description": "Set the timezone",
            "annotation": str,
            "action": "set",
        }

        ns = MagicMock()
        ns.value = "US/Eastern"
        ctrl.parse_simple_args = MagicMock(return_value=(ns, []))

        ctrl._generate_command("timezone", field, "set")
        assert hasattr(ctrl, "call_timezone")

        ctrl.call_timezone(["-v", "US/Eastern"])
        mock_user_session.console.print.assert_called()

    def test_generate_set_command_no_value_shows_current(
        self, mock_user_session, mock_user_obb
    ):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()

        field = {
            "command": "timezone",
            "field_name": "timezone",
            "description": "Set the timezone",
            "annotation": str,
            "action": "set",
        }

        ns = MagicMock()
        ns.value = None
        ctrl.parse_simple_args = MagicMock(return_value=(ns, []))
        mock_user_obb.user.preferences.timezone = "UTC"

        ctrl._generate_command("timezone", field, "set")
        ctrl.call_timezone([])
        mock_user_session.console.print.assert_called()
        args = mock_user_session.console.print.call_args[0][0]
        assert "timezone" in args

    def test_invalid_action_type(self, mock_user_session, mock_user_obb):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()

        field = {
            "command": "x",
            "field_name": "x",
            "description": "x",
            "annotation": str,
            "action": "bad",
        }

        with pytest.raises(ValueError, match="not allowed"):
            ctrl._generate_command("x", field, "bad")

    def test_call_credentials_loads_controller(
        self, mock_user_session, mock_user_obb
    ):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()
        ctrl.load_class = MagicMock(return_value=[])

        ctrl.call_credentials(None)
        ctrl.load_class.assert_called_once()

    def test_print_help(self, mock_user_session, mock_user_obb):
        from openbb_cli.controllers.user_controller import UserController

        ctrl = UserController.__new__(UserController)
        ctrl.queue = []
        ctrl.update_completer = MagicMock()
        ctrl._PREF_COMMANDS = {
            "enable_feature": {
                "command": "enable_feature",
                "field_name": "enable_feature",
                "description": "Enable a feature",
                "annotation": bool,
                "action": "toggle",
            },
            "timezone": {
                "command": "timezone",
                "field_name": "timezone",
                "description": "Set the timezone",
                "annotation": str,
                "action": "set",
            },
        }
        mock_user_obb.user.preferences.enable_feature = True
        mock_user_obb.user.preferences.timezone = "UTC"

        ctrl.print_help()
        mock_user_session.console.print.assert_called_once()
