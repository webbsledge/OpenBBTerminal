"""Test the ArgparseClassProcessor — exercises class scanning, paths, and reference reads."""

from unittest.mock import MagicMock, patch

from openbb_cli.argparse_translator.argparse_class_processor import (
    ArgparseClassProcessor,
)


class _FakeContainer:
    """Stand-in mimicking ``openbb_core.app.static.container.Container`` for isinstance checks.

    The processor uses ``isinstance(member, Container)`` to detect nested
    namespaces; we monkey-patch the imported ``Container`` reference so our
    fake counts.
    """


class _NestedTarget:
    """Top-level target with one direct method and one nested namespace."""

    def __init__(self):
        self.nested = _SubTarget()

    def list_things(self) -> dict:
        """Top-level method."""
        return {}


class _SubTarget(_FakeContainer):
    """Nested namespace with its own method (recursion case for ``_process_class``)."""

    def detail(self) -> dict:
        """Nested method."""
        return {}


def _make_target_with_method_and_container() -> _NestedTarget:
    return _NestedTarget()


def test_get_translator_returns_registered_translator():
    """``get_translator`` looks up the named translator from the internal dict."""

    class Plain:
        def hello(self) -> dict:
            """Hello."""
            return {}

    target = Plain()
    with patch(
        "openbb_cli.argparse_translator.argparse_class_processor.Container",
        _FakeContainer,
    ):
        proc = ArgparseClassProcessor(target_class=target)
    assert proc.get_translator("plain_hello") is proc.translators["plain_hello"]


def test_process_class_recurses_into_container_members():
    """``_process_class`` recurses into namespaces flagged as ``Container``."""
    target = _make_target_with_method_and_container()
    with patch(
        "openbb_cli.argparse_translator.argparse_class_processor.Container",
        _FakeContainer,
    ):
        proc = ArgparseClassProcessor(target_class=target)
    assert any("detail" in name for name in proc.translators)


def test_build_paths_walks_nested_containers():
    """``_build_paths`` records the nested namespace name and depth."""
    target = _make_target_with_method_and_container()
    with patch(
        "openbb_cli.argparse_translator.argparse_class_processor.Container",
        _FakeContainer,
    ):
        proc = ArgparseClassProcessor(target_class=target)
    assert "nested" in proc.paths
    assert proc.paths["nested"] == "subpath"


def test_custom_groups_from_reference_pulls_route_when_present():
    """When reference contains ``/<class>/<method>``, ``ReferenceToArgumentsProcessor`` is invoked."""

    class Plain:
        def hello(self) -> dict:
            """Hello."""
            return {}

    fake_rp = MagicMock()
    fake_rp.custom_groups = {"/plain/hello": []}

    target = Plain()
    with (
        patch(
            "openbb_cli.argparse_translator.argparse_class_processor.Container",
            _FakeContainer,
        ),
        patch(
            "openbb_cli.argparse_translator.argparse_class_processor.ReferenceToArgumentsProcessor",
            return_value=fake_rp,
        ) as rp_class,
    ):
        ArgparseClassProcessor(
            target_class=target,
            reference={"/plain/hello": {"some": "reference-data"}},
        )
    rp_class.assert_called()
