"""Runtime warnings emitted by the ParclLabs SDK.

These are warnings rather than prints so that callers can filter, capture, or
escalate them:

    import warnings
    from parcllabs.warnings import ParclLabsTruncationWarning

    # silence intentional truncation
    warnings.filterwarnings("ignore", category=ParclLabsTruncationWarning)

    # or turn incomplete results into a hard failure
    warnings.filterwarnings("error", category=ParclLabsIncompleteResultWarning)
"""

import warnings


class ParclLabsWarning(UserWarning):
    """Base class for all ParclLabs SDK runtime warnings."""


class ParclLabsTruncationWarning(ParclLabsWarning):
    """More data matched the query than was returned.

    Emitted when an explicit ``limit`` capped the result below the number of
    matching properties. The returned data is correct, just partial -- this is
    what the caller asked for. Emitted once per session to stay usable inside
    per-market loops.
    """


class ParclLabsIncompleteResultWarning(ParclLabsWarning):
    """One or more pages could not be fetched, so the result is short.

    Unlike truncation, this is a failure rather than a request: the caller asked
    for data the SDK was unable to retrieve. Emitted on every affected call, and
    the failed page offsets are recorded in ``metadata["incomplete_pages"]``.
    """


# Truncation is a requested outcome, so it is announced once per process rather
# than on every call. A module-level flag is used deliberately: mutating the
# global `warnings` filters from library code would clobber caller configuration.
_truncation_warned = False


def warn_truncation(returned_count: int, total_available: int, stacklevel: int = 4) -> None:
    """Warn once per session that an explicit ``limit`` withheld matching data."""
    global _truncation_warned  # noqa: PLW0603
    if _truncation_warned:
        return
    _truncation_warned = True
    warnings.warn(
        f"Returned {returned_count:,} of {total_available:,} matching properties because "
        f"`limit` capped the result. Raise `limit` to retrieve more, or omit it entirely "
        f"to retrieve all {total_available:,}. Credits are charged per property returned. "
        f"Compare metadata['results']['returned_count'] against "
        f"metadata['results']['total_available'] to detect this programmatically. "
        f"(This warning is shown once per session.)",
        ParclLabsTruncationWarning,
        stacklevel=stacklevel,
    )


def warn_incomplete_pages(
    failed_offsets: list[int], expected: int, retrieved: int, stacklevel: int = 4
) -> None:
    """Warn that pagination could not fetch every page. Fires on every occurrence."""
    warnings.warn(
        f"Incomplete result: {len(failed_offsets)} page(s) failed after retries, so "
        f"{retrieved:,} of an expected {expected:,} properties were retrieved. Failed "
        f"offsets are listed in metadata['incomplete_pages']. Re-run those offsets or "
        f"retry the query before treating this data as complete. Failed offsets: "
        f"{failed_offsets}",
        ParclLabsIncompleteResultWarning,
        stacklevel=stacklevel,
    )


def warn_integrity_mismatch(unique_properties: int, expected: int, stacklevel: int = 4) -> None:
    """Warn that assembled pages did not yield the expected number of properties."""
    warnings.warn(
        f"Pagination integrity check failed: assembled {unique_properties:,} unique "
        f"properties but expected {expected:,}. This can indicate overlapping or skipped "
        f"pages (offset pagination relies on a stable server-side sort). The data is "
        f"returned as-is; verify before use and report this to team@parcllabs.com.",
        ParclLabsIncompleteResultWarning,
        stacklevel=stacklevel,
    )


def _reset_truncation_warning() -> None:
    """Reset the once-per-session truncation flag. Intended for tests."""
    global _truncation_warned  # noqa: PLW0603
    _truncation_warned = False
