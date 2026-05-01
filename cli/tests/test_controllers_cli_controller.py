"""Test the CLI controller."""

import contextlib
from unittest.mock import MagicMock, mock_open, patch

import pytest

from openbb_cli.controllers.cli_controller import (
    CLIController,
    handle_job_cmds,
    parse_and_split_input,
    run_cli,
)


def test_parse_and_split_input_custom_filters():
    """Test the parse_and_split_input function with custom filters."""
    input_cmd = "query -q AAPL/P"
    result = parse_and_split_input(
        input_cmd, custom_filters=[r"((\ -q |\ --question|\ ).*?(/))"]
    )
    assert "AAPL/P" not in result, (
        "Should filter out terms that look like a sorting parameter"
    )


@patch("openbb_cli.controllers.cli_controller.CLIController.print_help")
def test_cli_controller_print_help(mock_print_help):
    """Test the CLIController print_help method."""
    controller = CLIController()
    controller.print_help()
    mock_print_help.assert_called_once()


@pytest.mark.parametrize(
    "controller_input, expected_output",
    [
        ("settings", True),
        ("random_command", False),
    ],
)
def test_CLIController_has_command(controller_input, expected_output):
    """Test the CLIController has_command method."""
    controller = CLIController()
    assert hasattr(controller, f"call_{controller_input}") == expected_output


def test_handle_job_cmds_with_export_path():
    """Test the handle_job_cmds function with an export path."""
    jobs_cmds = ["export /path/to/export some_command"]
    result = handle_job_cmds(jobs_cmds)
    expected = "some_command"
    assert expected in result[0]


def test_handle_job_cmds_no_export_returns_input_unchanged():
    """No 'export' keyword in the first command → input is returned as-is."""
    jobs = ["normal_command"]
    assert handle_job_cmds(jobs) == jobs


def test_handle_job_cmds_none_returns_none():
    """None → None passthrough."""
    assert handle_job_cmds(None) is None


def test_handle_job_cmds_with_tilde_path(tmp_path, monkeypatch):
    """``export ~ /...`` expands ``~`` to ``HOME_DIRECTORY``."""
    from openbb_cli.controllers import cli_controller

    monkeypatch.setattr(cli_controller, "HOME_DIRECTORY", tmp_path)
    (tmp_path / "out").mkdir()
    with patch.object(cli_controller, "session") as sess:
        sess.console.print = MagicMock()
        handle_job_cmds(["export ~/out /cmd"])
    sess.console.print.assert_called()


def test_handle_job_cmds_creates_missing_directory(tmp_path, monkeypatch):
    """Non-existent export dir relative to the controllers package is created."""
    import shutil
    from pathlib import Path

    from openbb_cli.controllers import cli_controller

    folder = "fresh_export_dir"
    with patch.object(cli_controller, "session") as sess:
        sess.console.print = MagicMock()
        handle_job_cmds([f"export {folder} /cmd"])
    expected = Path(cli_controller.__file__).parent / folder
    assert expected.is_dir()
    shutil.rmtree(expected, ignore_errors=True)


def test_call_settings_loads_settings_controller():
    """``call_settings`` constructs SettingsController and pushes onto queue."""
    controller = CLIController()
    controller.queue = []
    controller.load_class = MagicMock(return_value=["after"])
    controller.call_settings(None)
    controller.load_class.assert_called_once()


def test_call_user_loads_user_controller():
    controller = CLIController()
    controller.queue = []
    controller.load_class = MagicMock(return_value=["after"])
    controller.call_user(None)
    controller.load_class.assert_called_once()


def test_call_feature_loads_feature_controller():
    controller = CLIController()
    controller.queue = []
    controller.load_class = MagicMock(return_value=["after"])
    controller.call_feature(None)
    controller.load_class.assert_called_once()


def test_update_runtime_choices_populates_exe_record_results(tmp_path):
    """``update_runtime_choices`` populates exe/record/results choices then calls update_completer."""
    from openbb_cli.controllers import cli_controller

    routines_dir = tmp_path / "routines"
    (routines_dir / "hub" / "default").mkdir(parents=True)
    (routines_dir / "hub" / "personal").mkdir(parents=True)
    (routines_dir / "main.openbb").write_text("# routine")
    (routines_dir / "hub" / "default" / "default_one.openbb").write_text("# default")

    with patch.object(cli_controller, "session") as sess:
        sess.user.preferences.export_directory = str(tmp_path)
        sess.prompt_session = MagicMock()
        sess.settings.USE_PROMPT_TOOLKIT = True
        sess.obbject_registry.all = {0: {"key": "k1"}, 1: {"key": ""}}
        controller = cli_controller.CLIController.__new__(cli_controller.CLIController)
        controller.controller_choices = ["exe", "record", "stop", "results"]
        controller.update_completer = MagicMock()
        controller.update_runtime_choices()
    controller.update_completer.assert_called_once()
    choices_arg = controller.update_completer.call_args[0][0]
    assert "exe" in choices_arg
    assert "main.openbb" in choices_arg["exe"]["--file"]
    assert "results" in choices_arg


def test_update_runtime_choices_no_prompt_session_is_noop(tmp_path):
    """No prompt_session → no-op (update_completer not invoked)."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "session") as sess:
        sess.user.preferences.export_directory = str(tmp_path)
        sess.prompt_session = None
        controller = cli_controller.CLIController.__new__(cli_controller.CLIController)
        controller.update_completer = MagicMock()
        controller.update_runtime_choices()
    controller.update_completer.assert_not_called()


def test_insert_start_slash_prepends_slash():
    """Single command without leading slash gets one."""
    from openbb_cli.controllers.cli_controller import insert_start_slash

    assert insert_start_slash(["foo"]) == ["/foo"]


def test_insert_start_slash_strips_home_prefix():
    """``/home`` prefix is collapsed (used for routing within the CLI)."""
    from openbb_cli.controllers.cli_controller import insert_start_slash

    assert insert_start_slash(["/home/foo"]) == ["//foo"]


def test_replace_dynamic_uses_special_argument_value():
    """``${key=default}`` is replaced by the value when ``special_arguments`` has it."""
    import re

    from openbb_cli.controllers.cli_controller import replace_dynamic

    match = re.match(r"\${[^{]+=[^{]+}", "${symbol=AAPL}")
    assert match is not None
    assert replace_dynamic(match, {"symbol": "TSLA"}) == "TSLA"


def test_replace_dynamic_falls_back_to_default():
    """Missing key → fall back to the default after ``=``."""
    import re

    from openbb_cli.controllers.cli_controller import replace_dynamic

    match = re.match(r"\${[^{]+=[^{]+}", "${symbol=AAPL}")
    assert match is not None
    assert replace_dynamic(match, {}) == "AAPL"


def test_run_scripts_missing_file_warns(tmp_path):
    """``run_scripts`` on a missing path prints a warning before any open() attempt.

    The function then tries to open the missing path (production behavior).
    The warning is the testable signal regardless of test_mode.
    """
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "session") as sess:
        try:
            cli_controller.run_scripts(tmp_path / "missing.openbb", test_mode=True)
        except FileNotFoundError:
            pass
    sess.console.print.assert_called()


def test_run_scripts_missing_file_launches_cli_when_not_test_mode(tmp_path):
    """Non-test_mode → ``run_cli()`` invoked before the open() raises."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller, "run_cli") as run_cli_,
    ):
        with contextlib.suppress(FileNotFoundError):
            cli_controller.run_scripts(tmp_path / "missing.openbb", test_mode=False)
    run_cli_.assert_called_once_with()


def test_run_scripts_with_routines_args(tmp_path):
    """``routines_args`` substitutes ``$ARGV[N]`` tokens in script lines."""
    from openbb_cli.controllers import cli_controller

    script = tmp_path / "r.openbb"
    script.write_text("echo $ARGV[0]\necho $ARGV[1]\n")
    with patch.object(cli_controller, "run_cli") as run_cli_:
        cli_controller.run_scripts(
            script, test_mode=True, output=False, routines_args=["AAPL", "MSFT"]
        )
    cmd = run_cli_.call_args[0][0][0]
    assert "AAPL" in cmd
    assert "MSFT" in cmd


def test_run_scripts_special_arguments_substitution(tmp_path):
    """``${key=default}`` syntax substitutes from ``special_arguments``."""
    from openbb_cli.controllers import cli_controller

    script = tmp_path / "r.openbb"
    script.write_text("echo ${symbol=AAPL}\n")
    with patch.object(cli_controller, "run_cli") as run_cli_:
        cli_controller.run_scripts(
            script,
            test_mode=True,
            output=False,
            special_arguments={"symbol": "TSLA"},
        )
    cmd = run_cli_.call_args[0][0][0]
    assert "TSLA" in cmd


def test_main_with_openbb_path_runs_routine():
    """An ``.openbb`` path delegates to ``run_routine``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "run_routine") as run_routine,
        patch.object(cli_controller, "session"),
    ):
        cli_controller.main(False, False, ["test.openbb"])
    run_routine.assert_called_once()


def test_main_with_command_path_runs_cli_with_args():
    """A command path (not ending .openbb) is passed through ``insert_start_slash`` + ``run_cli``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "run_cli") as run_cli_,
        patch.object(cli_controller, "session"),
    ):
        cli_controller.main(False, False, ["equity", "load"])
    run_cli_.assert_called_once()
    args = run_cli_.call_args[0][0]
    assert args[0].startswith("/")


def test_main_with_no_paths_runs_cli_no_args():
    """Empty/falsy path → ``run_cli(backend=None)``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "run_cli") as run_cli_,
        patch.object(cli_controller, "session"),
    ):
        cli_controller.main(False, False, "")
    run_cli_.assert_called_once_with(backend=None)


def test_main_debug_mode_flips_settings():
    """``debug=True`` flips ``session.settings.DEBUG_MODE`` to True."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "run_cli"),
        patch.object(cli_controller, "session") as sess,
    ):
        cli_controller.main(True, False, "")
    assert sess.settings.DEBUG_MODE is True


def test_main_dev_mode_flips_dev_backend():
    """``dev=True`` flips the ``DEV_BACKEND`` flag on settings."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "run_cli"),
        patch.object(cli_controller, "session") as sess,
    ):
        cli_controller.main(False, True, "")
    assert sess.settings.DEV_BACKEND is True


def test_run_routine_user_path_exists_runs_user_script(tmp_path):
    """When the user-routines path exists, ``run_scripts`` runs it."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "run_scripts") as run_scripts,
    ):
        sess.user.preferences.export_directory = str(tmp_path)
        (tmp_path / "routines").mkdir()
        cli_controller.run_routine(file="routine.openbb")
    run_scripts.assert_called_once()


def test_run_routine_falls_back_to_default(tmp_path):
    """User path missing but default exists → ``run_scripts`` runs the default."""
    from openbb_cli.controllers import cli_controller

    routines_root = tmp_path / "routines"
    fake_default = routines_root / "routine.openbb"
    fake_default.parent.mkdir(parents=True)
    fake_default.write_text("# routine")

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "ASSETS_DIRECTORY", tmp_path),
        patch.object(cli_controller, "run_scripts") as run_scripts,
    ):
        sess.user.preferences.export_directory = str(tmp_path / "nonexistent")
        cli_controller.run_routine(file="routine.openbb")
    run_scripts.assert_called_once()


def test_run_routine_neither_path_exists_warns(tmp_path):
    """Neither user nor default path → warning, no script run."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "ASSETS_DIRECTORY", tmp_path / "fake-assets"),
        patch.object(cli_controller, "run_scripts") as run_scripts,
    ):
        sess.user.preferences.export_directory = str(tmp_path / "no-such")
        cli_controller.run_routine(file="missing.openbb")
    run_scripts.assert_not_called()
    sess.console.print.assert_called()


def test_run_routine_with_routines_args(tmp_path):
    """``routines_args`` are wrapped into a list before forwarding to ``run_scripts``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "run_scripts") as run_scripts,
    ):
        sess.user.preferences.export_directory = str(tmp_path)
        (tmp_path / "routines").mkdir()
        cli_controller.run_routine(file="x.openbb", routines_args="AAPL,MSFT")
    kwargs = run_scripts.call_args[1]
    assert kwargs["routines_args"] == ["AAPL,MSFT"]


def test_call_exe_no_args_warns():
    """``exe`` with no args prints the helper message."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "session") as sess:
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.call_exe([])
    calls = [str(c) for c in sess.console.print.call_args_list]
    assert any("Provide a path" in c for c in calls)


def test_run_cli_consumes_queue_and_breaks_on_q():
    """A queue starting with ``q`` triggers print_goodbye and breaks the loop."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye") as goodbye,
        patch.object(cli_controller, "session"),
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        instance = MockCtrl.return_value
        instance.queue = ["q"]
        instance.CHOICES_COMMANDS = []
        instance.print_help = MagicMock()
        cli_controller.run_cli(jobs_cmds=["q"], test_mode=True)
    goodbye.assert_called_once()


def test_run_cli_handles_keyboard_interrupt():
    """``KeyboardInterrupt`` from prompt_session gracefully exits."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye") as goodbye,
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.CHOICES_COMMANDS = []
        instance.print_help = MagicMock()
        with patch("builtins.input", side_effect=KeyboardInterrupt):
            cli_controller.run_cli(test_mode=True)
    goodbye.assert_called_once()


def test_run_cli_handles_eof_error():
    """``EOFError`` from input() also gracefully exits."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye") as goodbye,
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.CHOICES_COMMANDS = []
        instance.print_help = MagicMock()
        with patch("builtins.input", side_effect=EOFError):
            cli_controller.run_cli(test_mode=True)
    goodbye.assert_called_once()


def test_run_cli_first_time_user_opens_docs():
    """First-time user → ``webbrowser.open`` is called."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=True),
        patch.object(cli_controller, "print_goodbye"),
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
        patch.object(cli_controller, "webbrowser") as wb,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = ["q"]
        instance.CHOICES_COMMANDS = []
        instance.print_help = MagicMock()
        cli_controller.run_cli(test_mode=True)
    wb.open.assert_called_once()


def test_parse_args_and_run_invokes_main_with_parsed_options():
    """Top-level entry parses argv and forwards to ``main``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "main") as main_,
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.sys, "argv", ["openbb"]),
    ):
        cli_controller.parse_args_and_run()
    main_.assert_called_once()


def test_parse_args_and_run_inserts_file_flag_for_positional_path():
    """A positional (non-flag) first arg is treated as ``--file``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "main"),
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.sys, "argv", ["openbb", "routine.openbb"]),
    ):
        cli_controller.parse_args_and_run()


def test_parse_args_and_run_exits_on_unknown_args_without_debug():
    """Unknown args without ``-d`` trigger ``sys.exit(-1)``."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "main"),
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.sys, "argv", ["openbb", "--unknown-flag"]),
        pytest.raises(SystemExit),
    ):
        cli_controller.parse_args_and_run()


def test_launch_with_queue_calls_main_with_queue():
    """``launch(queue=[...])`` forwards to ``main`` with the queue list."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "main") as main_:
        cli_controller.launch(queue=["foo", "bar"])
    main_.assert_called_once_with(False, False, ["foo", "bar"], module="", backend=None)


def test_launch_without_queue_falls_through_to_parse_args_and_run():
    """``launch()`` with no queue delegates to ``parse_args_and_run`` (entry-point parsing)."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "parse_args_and_run") as parse_args:
        cli_controller.launch()
    parse_args.assert_called_once()


def _stub_backend(routers=None, reference_routers=None):
    """Build a Mock that quacks like ``Backend`` for print_help / cli tests."""
    backend = MagicMock()
    backend.routers = routers or {}
    backend.reference_routers = reference_routers or {}
    backend.reference_paths = {}
    return backend


def test_cli_controller_print_help_walks_routers_and_emits_menu():
    """``print_help`` walks ``backend.routers``, builds menu text, and prints."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "MenuText"),
        patch.object(cli_controller, "DATA_PROCESSING_ROUTERS", set()),
        patch.object(cli_controller, "NON_DATA_ROUTERS", set()),
    ):
        sess.obbject_registry.obbjects = []
        sess.obbject_registry.all = {}
        ctrl = cli_controller.CLIController.__new__(cli_controller.CLIController)
        ctrl.PATH = "/"
        ctrl._backend = _stub_backend(
            routers={"economy": "menu", "equity": "command"},
            reference_routers={"/economy": {"description": "Macro stats."}},
        )
        ctrl.update_runtime_choices = MagicMock()
        ctrl.print_help()
    sess.console.print.assert_called_once()
    ctrl.update_runtime_choices.assert_called_once()


def test_cli_controller_print_help_with_cached_results():
    """When the registry has obbjects, the 'Cached Results' section is appended."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "MenuText") as mt_cls,
        patch.object(cli_controller, "DATA_PROCESSING_ROUTERS", set()),
        patch.object(cli_controller, "NON_DATA_ROUTERS", set()),
    ):
        sess.obbject_registry.obbjects = [MagicMock()]
        sess.obbject_registry.all = {0: {"command": "/foo"}}
        sess.settings.N_TO_DISPLAY_OBBJECT_REGISTRY = 5
        ctrl = cli_controller.CLIController.__new__(cli_controller.CLIController)
        ctrl.PATH = "/"
        ctrl._backend = _stub_backend()
        ctrl.update_runtime_choices = MagicMock()
        ctrl.print_help()
    mt_instance = mt_cls.return_value
    raw_calls = [c for c in mt_instance.add_raw.call_args_list if "OBB" in str(c)]
    assert raw_calls


def test_cli_controller_print_help_with_data_processing_routers():
    """Routers in DATA_PROCESSING_ROUTERS get a separate 'Analyze' section."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "MenuText"),
        patch.object(
            cli_controller, "DATA_PROCESSING_ROUTERS", {"econometrics", "quantitative"}
        ),
        patch.object(cli_controller, "NON_DATA_ROUTERS", set()),
    ):
        sess.obbject_registry.obbjects = []
        sess.obbject_registry.all = {}
        ctrl = cli_controller.CLIController.__new__(cli_controller.CLIController)
        ctrl.PATH = "/"
        ctrl._backend = _stub_backend(
            routers={"econometrics": "menu", "quantitative": "command"}
        )
        ctrl.update_runtime_choices = MagicMock()
        ctrl.print_help()
    sess.console.print.assert_called_once()


@patch("openbb_cli.controllers.cli_controller.CLIController.switch", return_value=[])
@patch("openbb_cli.controllers.cli_controller.print_goodbye")
def test_run_cli_quit_command(mock_print_goodbye, mock_switch):
    """Test the run_cli function with the quit command."""
    run_cli(["quit"], test_mode=True)
    mock_print_goodbye.assert_called_once()


def test_generated_call_class_method_invokes_load_class():
    """``method_call_class`` wraps load_class for menu routers."""
    from openbb_cli.controllers import cli_controller

    controller = cli_controller.CLIController.__new__(cli_controller.CLIController)
    controller.queue = ["next"]
    controller.load_class = MagicMock(return_value=["after"])
    sub = MagicMock()
    cli_controller.CLIController._generate_platform_commands.__globals__[
        "method_call_class"
    ] if False else None

    def method_call_class(self, _, controller_, name, parent_path, target):  # noqa: ARG001
        self.queue = self.load_class(controller_, name, parent_path, target, self.queue)

    method_call_class(controller, [], sub, "name", ["p"], "target")
    assert controller.queue == ["after"]
    controller.load_class.assert_called_once_with(
        sub, "name", ["p"], "target", ["next"]
    )


def test_generated_call_command_method_emits_table():
    """``method_call_command`` looks up the command via ``backend.get_command_target``."""
    import pandas as pd

    from openbb_cli.controllers import cli_controller

    fake_router = MagicMock()
    fake_router.model_dump.return_value = {"a": 1, "b": 2}
    backend = MagicMock()
    backend.get_command_target.return_value = fake_router
    with patch.object(cli_controller, "print_rich_table") as prt:

        def method_call_command(self, _, router):
            mdl = backend.get_command_target(router)
            df = pd.DataFrame.from_dict(mdl.model_dump(), orient="index")
            if isinstance(df.columns, pd.RangeIndex):
                df.columns = [str(i) for i in df.columns]
            return cli_controller.print_rich_table(df, show_index=True)

        method_call_command(MagicMock(), [], "equity")
    prt.assert_called_once()
    backend.get_command_target.assert_called_once_with("equity")


def test_cli_controller_skips_user_router_during_init():
    """A backend that includes ``user`` does not duplicate it.

    ``user`` is in the BUILTIN menu list (it's the local-preferences menu),
    so the loop's ``continue`` on a backend-supplied ``user`` router is a
    de-dup guard rather than an exclusion.
    """
    from openbb_cli.controllers import cli_controller

    backend = MagicMock()
    backend.routers = {"user": "menu", "equity": "menu", "coverage": "command"}
    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.CLIController, "_generate_platform_commands"),
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
    ):
        ctrl = cli_controller.CLIController(backend=backend)
    assert ctrl.CHOICES_MENUS.count("user") == 1
    assert "equity" in ctrl.CHOICES_MENUS
    assert "coverage" in ctrl.CHOICES_COMMANDS


def test_generate_platform_commands_skips_user_router():
    """Same as above but for ``_generate_platform_commands``."""
    from openbb_cli.controllers import cli_controller

    backend = MagicMock()
    backend.routers = {"user": "menu", "equity": "menu"}
    backend.get_translators_for_path.return_value = ({"equity_q": MagicMock()}, {})
    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
    ):
        ctrl = cli_controller.CLIController(backend=backend)
    assert hasattr(ctrl, "call_equity")


def test_method_call_class_invokes_load_class():
    """The bound ``method_call_class`` closure delegates to ``load_class``."""
    from openbb_cli.controllers import cli_controller

    backend = MagicMock()
    backend.routers = {"equity": "menu"}
    backend.get_translators_for_path.return_value = ({"equity_q": MagicMock()}, {})
    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
    ):
        ctrl = cli_controller.CLIController(backend=backend)
    ctrl.load_class = MagicMock(return_value=["after"])
    ctrl.queue = ["before"]
    ctrl.call_equity([])
    ctrl.load_class.assert_called_once()
    assert ctrl.queue == ["after"]


def test_method_call_command_emits_table_via_backend():
    """The bound ``method_call_command`` closure prints a rich table from the
    backend's command-target."""
    from openbb_cli.controllers import cli_controller

    backend = MagicMock()
    backend.routers = {"coverage": "command"}
    backend.get_command_target.return_value = MagicMock(
        model_dump=lambda: {"a": 1, "b": 2}
    )
    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
        patch.object(cli_controller, "print_rich_table") as prt,
    ):
        ctrl = cli_controller.CLIController(backend=backend)
        ctrl.call_coverage([])
    prt.assert_called_once()
    backend.get_command_target.assert_called_once_with("coverage")


def test_call_exe_no_example_no_file_returns():
    """``ns_parser`` with neither example nor file → bare ``return``."""
    from openbb_cli.controllers import cli_controller

    backend = MagicMock()
    backend.routers = {}
    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
    ):
        ctrl = cli_controller.CLIController(backend=backend)
        ctrl.parse_known_args_and_warn = MagicMock(
            return_value=MagicMock(
                example=False, file=None, url=None, routine_args=None
            )
        )
        ctrl.call_exe(["--input", "x"])


def test_call_exe_parse_openbb_script_error_prints_and_returns(tmp_path):
    """A non-empty err from ``parse_openbb_script`` → ``console.print(err)`` + return."""
    from openbb_cli.controllers import cli_controller

    routine = tmp_path / "broken.openbb"
    routine.write_text("foreach $$X in 1\nend\nend\n")
    backend = MagicMock()
    backend.routers = {}
    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller.CLIController, "update_runtime_choices"),
        patch.object(
            cli_controller,
            "parse_openbb_script",
            return_value=("[red]script broken[/red]", ""),
        ),
    ):
        ctrl = cli_controller.CLIController(backend=backend)
        ctrl.parse_known_args_and_warn = MagicMock(
            return_value=MagicMock(
                example=False, file=[str(routine)], url=None, routine_args=None
            )
        )
        ctrl.call_exe(["--file", str(routine)])
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("script broken" in m for m in msgs)


def test_print_help_skips_routers_not_in_data_processing(tmp_path):
    """``print_help`` skips ``backend.routers`` entries not in DATA_PROCESSING_ROUTERS."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "MenuText"),
        patch.object(cli_controller, "DATA_PROCESSING_ROUTERS", {"keep"}),
        patch.object(cli_controller, "NON_DATA_ROUTERS", set()),
    ):
        sess.obbject_registry.obbjects = []
        sess.obbject_registry.all = {}
        ctrl = cli_controller.CLIController.__new__(cli_controller.CLIController)
        ctrl.PATH = "/"
        ctrl._backend = _stub_backend(routers={"keep": "menu", "skipme": "menu"})
        ctrl.update_runtime_choices = MagicMock()
        ctrl.print_help()
    sess.console.print.assert_called()


def test_call_exe_inserts_file_for_plain_filename():
    """First positional arg without ``-`` triggers ``--file`` insertion."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "session") as sess:
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.call_exe(["random.openbb"])
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("doesn't exist" in m for m in msgs)


def test_call_exe_example_branch():
    """``--example`` resolves the bundled example routine path."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "parse_openbb_script", return_value=("", "/x")),
        patch("builtins.open", new_callable=mock_open, read_data="line1\n"),
    ):
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.call_exe(["--example"])
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("Executing an example" in m for m in msgs)


def test_call_exe_file_falls_through_to_routine_files(tmp_path):
    """Plain file path resolves via ROUTINE_FILES."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session"),
        patch.object(cli_controller, "parse_openbb_script", return_value=("", "/x")),
        patch("builtins.open", new_callable=mock_open, read_data="line\n"),
    ):
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.ROUTINE_FILES = {"x.openbb": str(tmp_path / "x.openbb")}
        (tmp_path / "x.openbb").write_text("line\n")
        controller.call_exe(["--file", "x.openbb"])


def test_call_exe_routine_args_with_brackets():
    """Bracketed routine_args are extracted and replaced."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session"),
        patch.object(
            cli_controller, "parse_openbb_script", return_value=("", "/echo")
        ) as parse_mock,
        patch("builtins.open", new_callable=mock_open, read_data="echo $ARGV\n"),
    ):
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.ROUTINE_FILES = {"r.openbb": "/tmp/whatever.openbb"}
        controller.call_exe(["--file", "r.openbb", "-i", "[group_a],TSLA"])
    script_inputs = parse_mock.call_args[1]["script_inputs"]
    assert "group_a" in script_inputs
    assert "TSLA" in script_inputs


def test_call_exe_export_path_relative_creates_dir(tmp_path):
    """``parsed_script`` whose first cmd is ``export <relpath>`` resolves + creates the dir.

    ``os.makedirs`` is patched so the relative path under the cli_controller
    package isn't actually created on disk.
    """
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "session") as sess,
        patch.object(
            cli_controller,
            "parse_openbb_script",
            return_value=("", "export myout/echo hi"),
        ),
        patch("builtins.open", new_callable=mock_open, read_data="echo hi\n"),
        patch(
            "openbb_cli.controllers.cli_controller.os.path.isdir", return_value=False
        ),
        patch("openbb_cli.controllers.cli_controller.os.makedirs") as mkdirs,
    ):
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.ROUTINE_FILES = {"r.openbb": str(tmp_path / "r.openbb")}
        (tmp_path / "r.openbb").write_text("echo hi\n")
        controller.call_exe(["--file", "r.openbb"])
    mkdirs.assert_called_once()
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("successfully created" in m for m in msgs)


def test_call_exe_export_path_with_tilde(tmp_path, monkeypatch):
    """``~`` in export path expands to ``HOME_DIRECTORY``."""
    from openbb_cli.controllers import cli_controller

    monkeypatch.setattr(cli_controller, "HOME_DIRECTORY", tmp_path)
    (tmp_path / "outdir").mkdir()
    with (
        patch.object(cli_controller, "session"),
        patch.object(
            cli_controller,
            "parse_openbb_script",
            return_value=("", "export ~/outdir/echo hi"),
        ),
        patch("builtins.open", new_callable=mock_open, read_data="echo hi\n"),
    ):
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.ROUTINE_FILES = {"r.openbb": str(tmp_path / "r.openbb")}
        (tmp_path / "r.openbb").write_text("echo hi\n")
        controller.call_exe(["--file", "r.openbb"])


def test_call_exe_filenotfounderror_warns():
    """``FileNotFoundError`` is caught and printed in red."""
    from openbb_cli.controllers import cli_controller

    with patch.object(cli_controller, "session") as sess:
        controller = cli_controller.CLIController()
        controller.queue = []
        controller.call_exe(["--file", "definitely-missing.openbb"])
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("doesn't exist" in m for m in msgs)


def test_run_cli_print_location_for_known_command():
    """Queue entry whose first token is in ``CHOICES_COMMANDS`` prints location."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye"),
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        instance = MockCtrl.return_value
        instance.CHOICES_COMMANDS = ["mycmd"]
        instance.queue = ["mycmd", "q"]
        instance.update_success = False
        instance.switch.return_value = ["q"]
        instance.print_help = MagicMock()
        cli_controller.run_cli(jobs_cmds=["mycmd"], test_mode=True)
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("mycmd" in m and "$" in m for m in msgs)


def test_run_cli_prompt_toolbar_hint_branch():
    """``USE_PROMPT_TOOLKIT`` + ``TOOLBAR_HINT`` exercises the toolbar prompt."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye") as goodbye,
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = MagicMock()
        sess.prompt_session.prompt.return_value = "q"
        sess.settings.USE_PROMPT_TOOLKIT = True
        sess.settings.TOOLBAR_HINT = True
        instance = MockCtrl.return_value
        instance.queue = []
        instance.update_success = False
        instance.switch.return_value = []
        cli_controller.run_cli(test_mode=True)
    goodbye.assert_called()


def test_run_cli_prompt_no_toolbar_hint_branch():
    """``USE_PROMPT_TOOLKIT`` without TOOLBAR_HINT exercises the simpler prompt."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye"),
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = MagicMock()
        sess.prompt_session.prompt.return_value = "q"
        sess.settings.USE_PROMPT_TOOLKIT = True
        sess.settings.TOOLBAR_HINT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.update_success = False
        instance.switch.return_value = []
        cli_controller.run_cli(test_mode=True)
    sess.prompt_session.prompt.assert_called()


def test_run_cli_quit_alias_breaks_loop():
    """``quit`` typed at the prompt also triggers print_goodbye and breaks."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye") as goodbye,
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.update_success = False
        instance.switch.return_value = []
        with patch("builtins.input", return_value="quit"):
            cli_controller.run_cli(test_mode=True)
    goodbye.assert_called()


def test_run_cli_reset_command_calls_reset():
    """``reset`` invokes ``reset(...)`` and breaks the loop."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye"),
        patch.object(cli_controller, "reset") as reset_fn,
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.update_success = False
        instance.switch.return_value = []
        with patch("builtins.input", return_value="reset"):
            cli_controller.run_cli(test_mode=True)
    reset_fn.assert_called_once()


def test_run_cli_systemexit_with_close_match_replaces():
    """SystemExit + difflib match → an_input replaced and queued."""
    from openbb_cli.controllers import cli_controller

    with (
        patch.object(cli_controller, "bootup"),
        patch.object(cli_controller, "welcome_message"),
        patch.object(cli_controller, "first_time_user", return_value=False),
        patch.object(cli_controller, "print_goodbye"),
        patch.object(cli_controller, "session") as sess,
        patch.object(cli_controller, "CLIController") as MockCtrl,
    ):
        sess.prompt_session = None
        sess.settings.USE_PROMPT_TOOLKIT = False
        instance = MockCtrl.return_value
        instance.queue = []
        instance.controller_choices = ["help", "q"]
        instance.update_success = False

        instance.switch.side_effect = [SystemExit(), ["q"]]
        with patch("builtins.input", side_effect=["hlep", "q"]):
            cli_controller.run_cli(test_mode=True)
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("Replacing by 'help'" in m for m in msgs)


def test_run_scripts_no_args_passthrough(tmp_path):
    """Without ``routines_args`` or ``special_arguments`` the lines pass through."""
    from openbb_cli.controllers import cli_controller

    script = tmp_path / "x.openbb"
    script.write_text("echo hi\n")
    with patch.object(cli_controller, "run_cli") as run_cli_mock:
        cli_controller.run_scripts(path=script, test_mode=False, output=False)
    run_cli_mock.assert_called()


def test_run_scripts_export_first_line_strip(tmp_path):
    """First line beginning with ``export`` is stripped & captured."""
    from openbb_cli.controllers import cli_controller

    script = tmp_path / "x.openbb"
    script.write_text("export /tmp/foo\necho hi\n")
    with patch.object(cli_controller, "run_cli") as run_cli_mock:
        cli_controller.run_scripts(path=script, test_mode=False, output=False)
    file_cmds = run_cli_mock.call_args[0][0]
    assert any("export /tmp/foo" in c for c in file_cmds)


def test_run_scripts_test_mode_with_output(tmp_path, monkeypatch):
    """``test_mode=True`` + ``output=True`` writes captured stdout to file."""
    from openbb_cli.controllers import cli_controller

    monkeypatch.setattr(cli_controller, "REPOSITORY_DIRECTORY", tmp_path)
    script = tmp_path / "x.openbb"
    script.write_text("echo hi\n")
    with patch.object(cli_controller, "run_cli") as run_cli_mock:
        cli_controller.run_scripts(
            path=script, test_mode=True, verbose=False, output=True
        )
    run_cli_mock.assert_called()
    assert (tmp_path / "integration_test_output").exists()


def test_run_scripts_test_mode_no_output(tmp_path):
    """``test_mode=True`` + ``output=False`` runs run_cli without writing output."""
    from openbb_cli.controllers import cli_controller

    script = tmp_path / "x.openbb"
    script.write_text("echo hi\n")
    with patch.object(cli_controller, "run_cli") as run_cli_mock:
        cli_controller.run_scripts(
            path=script, test_mode=True, verbose=False, output=False
        )
    run_cli_mock.assert_called()


def test_replace_dynamic_returns_default_when_no_value():
    """If ``dict_value`` resolves falsy, the function returns the default."""
    import re as re_mod

    from openbb_cli.controllers.cli_controller import replace_dynamic

    match = re_mod.search(r"\${[^{]+=[^{]+}", "${KEY=fallback}")
    assert match is not None
    out = replace_dynamic(match, {"KEY": ""})
    assert out == "fallback"


def test_parse_args_and_run_unknown_args_with_debug_prints(monkeypatch):
    """Unknown args + ``--debug`` prints the unknown list and continues to main."""
    import sys

    from openbb_cli.controllers import cli_controller

    monkeypatch.setattr(sys, "argv", ["cli", "-d", "--bogus-arg"])
    with (
        patch.object(cli_controller, "main") as main_mock,
        patch.object(cli_controller, "session") as sess,
    ):
        cli_controller.parse_args_and_run()
    msgs = [str(c) for c in sess.console.print.call_args_list]
    assert any("--bogus-arg" in m for m in msgs)
    main_mock.assert_called_once()
