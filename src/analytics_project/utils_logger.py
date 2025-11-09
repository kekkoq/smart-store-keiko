"""Compatibility shim for the project's logger.

Historically some modules and tests import the logger as

    from analytics_project import utils_logger

but the implementation lives under `analytics_project.utils.logger`.
This thin shim re-exports the expected symbols so both import styles
continue to work without changing call sites.
"""

from .utils.logger import (
    get_log_file_path,
    init_logger,
    log_example,
    logger,
    main,
)

__all__ = ["get_log_file_path", "init_logger", "log_example", "logger", "main"]

if __name__ == "__main__":
    log_example()
