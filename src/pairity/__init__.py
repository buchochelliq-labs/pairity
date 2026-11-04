"""Pairity's configuration API has no Spark or workspace dependency."""

from pairity.config import SuiteConfig
from pairity.serialization import dump_config, load_config

__version__ = "0.1.0.dev0"
__all__ = ["SuiteConfig", "dump_config", "load_config"]
