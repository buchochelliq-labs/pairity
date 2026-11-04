import pytest

from pairity.plugins import PluginRegistry


def test_registered_factory_constructs_fresh_components():
    class Adapter:
        api_version = 1

    registry = PluginRegistry()
    registry.register("custom", Adapter)
    assert isinstance(registry.create("custom"), Adapter)
    assert registry.create("custom") is not registry.create("custom")


def test_duplicate_unknown_or_incompatible_plugins_do_not_fallback():
    class Adapter:
        api_version = 1

    class Unsupported:
        api_version = 2

    registry = PluginRegistry()
    registry.register("known", Adapter)
    with pytest.raises(ValueError, match="already"):
        registry.register("known", Adapter)
    with pytest.raises(ValueError, match="Unknown"):
        registry.create("os.system")
    registry.register("newer", Unsupported)
    with pytest.raises(ValueError, match="version"):
        registry.create("newer")
