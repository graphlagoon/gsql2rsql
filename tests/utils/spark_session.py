"""Shared SparkSession infrastructure for PySpark tests.

A single JVM/SparkContext is created lazily per process and reused by all
test modules.  Each module gets an isolated ``SparkSession`` via
``newSession()`` (own temp-view catalog, own runtime SQL conf), preserving
module-level isolation without paying ~30 SparkContext restarts per run.

The base session is intentionally never stopped: ``spark.stop()`` would
kill the shared SparkContext for every subsequent module.  Managed tables
and databases share the metastore across child sessions; the autouse
fixture in tests/conftest.py resets that state between modules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_BASE: "SparkSession | None" = None
_HAS_SCRIPTING: bool | None = None


def _is_live(session: "SparkSession") -> bool:
    """True if the session's SparkContext has not been stopped."""
    try:
        return not session.sparkContext._jsc.sc().isStopped()
    except Exception:
        # Any failure probing the JVM means the context is unusable.
        return False


def get_base_spark() -> "SparkSession":
    """Return the process-wide base SparkSession, creating it on first use.

    Several older modules in ``tests/pyspark_tests`` still call
    ``spark.stop()`` in a fixture teardown (see the module docstring).  That
    kills the shared SparkContext, so a cached ``_BASE`` handed out afterwards
    raises ``IllegalStateException: Cannot call methods on a stopped
    SparkContext`` for every later module.  Rebuild rather than hand back a
    dead session -- correctness of later modules must not depend on
    alphabetical test ordering.
    """
    global _BASE
    if _BASE is not None and _is_live(_BASE):
        return _BASE
    _BASE = None
    from pyspark.sql import SparkSession

    _BASE = (
        SparkSession.builder
        .appName("gsql2rsql_tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.scripting.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    _BASE.sparkContext.setLogLevel("ERROR")
    return _BASE


def base_spark_or_none() -> "SparkSession | None":
    """Return the base session only if one was already started."""
    return _BASE


def new_test_session(conf: dict[str, str] | None = None) -> "SparkSession":
    """Isolated child session sharing the base SparkContext.

    Temp views and runtime SQL confs set on the returned session do not
    leak to other sessions.
    """
    session = get_base_spark().newSession()
    for key, value in (conf or {}).items():
        session.conf.set(key, value)
    return session


def has_pyspark_scripting() -> bool:
    """Probe SQL scripting support (PySpark >= 4.2) using the shared base.

    Import-time probes must not create-and-stop their own session: a
    ``stop()`` there would kill the shared context for later modules.
    """
    global _HAS_SCRIPTING
    if _HAS_SCRIPTING is None:
        try:
            get_base_spark().sql("BEGIN DECLARE x INT DEFAULT 1; END")
            _HAS_SCRIPTING = True
        except Exception:
            _HAS_SCRIPTING = False
    return _HAS_SCRIPTING
