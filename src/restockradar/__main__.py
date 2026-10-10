"""Process entrypoint: argument parsing (``--dry-run``, ``--once``), dependency
wiring, and run loop (Req 20).

For Milestone 1 this is the thin ``asyncio.run`` shell around
:func:`restockradar.app.run`, which parses ``--dry-run`` / ``--once`` (also honouring
``RR_DRY_RUN`` / ``RR_ONCE``) and runs ONE dry-run ``--once`` Baseline_Seeding cycle end
to end. Invoked without ``--once`` it prints a clear "M1 only supports --once --dry-run"
message and exits 0. The APScheduler loop, dispatch, watchdog, and real webhook POSTs
arrive in later milestones.
"""

from __future__ import annotations

import asyncio
import sys

from restockradar.app import run


def main(argv: list[str] | None = None) -> int:
    """Console-script entrypoint: run the M1 ``--once --dry-run`` cycle and return 0.

    Returns the process exit code (0 on success), so both ``python -m restockradar`` and
    the ``restockradar`` console script report the right status.
    """
    return asyncio.run(run(argv))


if __name__ == "__main__":
    sys.exit(main())
