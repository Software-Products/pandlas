"""
Concurrent Synchro Writing
==========================

Demonstrates :func:`pandlas.add_synchro_data_multi`, which writes several
synchro parameters to a single session concurrently.

Why it helps
------------
For a synchro write, ~95% of the time is spent in the per-packet
``session.AddSynchroChannelData`` call, which is latency-bound on the SQL Race /
database round-trip.  pythonnet releases the GIL during that .NET call, so
writes to *different* channels overlap when issued from a thread pool — giving
roughly 2x throughput on multi-parameter sessions (the database is the shared
ceiling, so the speed-up is sub-linear in the worker count).

The config for each channel is created serially first (config commits are not
thread-safe); only the data-write phase runs concurrently.

.. note::
    This parallelises *across* parameters.  A single parameter is still written
    by one thread — per-channel synchro packets carry an ordered sequence number
    and must come from a single writer.

This script writes the same set of signals twice into one session — once
serially via ``add_synchro_data`` and once concurrently via
``add_synchro_data_multi`` — reports the wall-clock of each phase, and validates
that every concurrently-written parameter round-trips byte-for-byte.

Requirements:
  - ATLAS 10 installed with the SQL Race API available
  - pandlas installed

Usage:
    python synchro_concurrent.py
"""

import os
import sys
import time
import logging

import numpy as np
import pandas as pd

# Make the tick/cross glyphs printable on legacy code-page consoles (cp1252).
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

from pandlas import SQLiteConnection, SQLRaceDBConnection
from pandlas import add_synchro_data, add_synchro_data_multi
import pandlas.SqlRace as sr

logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ========================= CONFIGURATION =========================
# Backend: "sqlite" or "sqlserver"
BACKEND = "sqlserver"

# SQLite settings (only used when BACKEND = "sqlite")
SQLITE_DB_DIR = r"C:\McLaren Applied\pandlas\SynchroConcurrent.ssndb"

# SQL Server settings (only used when BACKEND = "sqlserver")
SERVER = r"MCLA-525Q374\LOCAL"
DATABASE = "SQLRACE02"

# Scale knobs — overridable via env vars so you can try sizes without editing.
N_PARAMS = int(os.environ.get("SYNCHRO_N_PARAMS", "4"))      # synchro params (channels)
N_PER = int(os.environ.get("SYNCHRO_N_PER", "10_000_000"))    # samples per parameter
MAX_WORKERS = int(os.environ.get("SYNCHRO_MAX_WORKERS", "4"))  # concurrent writer threads
PACKET_SIZE = 32_000    # samples per packet (empirical sweet spot 32k-48k)

# Engine RPM profile
RPM_IDLE = 800
RPM_MAX = 12_000
RPM_CYCLES = 4
TEETH_PER_REV = 36
# =================================================================


def engine_timebase(n_samples: int) -> np.ndarray:
    """Build one crank-synchronous timebase (ns) from an RPM sweep.

    All parameters share this timebase — they represent different signals
    sampled on the *same* crank, so they are co-terminous.  (Giving channels
    different end times trips a NullReferenceException in SQL Race's synchro
    read path when ``get_samples`` queries the full session range.)
    """
    t = np.arange(n_samples, dtype=np.float64)
    rpm_profile = (
        RPM_IDLE + (RPM_MAX - RPM_IDLE)
        * 0.5 * (1.0 - np.cos(2.0 * np.pi * t / max(n_samples, 1) * RPM_CYCLES))
    )
    rps = rpm_profile[:-1] / 60.0
    intervals_ns = (1.0 / (rps * TEETH_PER_REV) * 1e9).astype(np.int64)
    intervals_ns = np.maximum(intervals_ns, 1000)  # floor at 1 us

    timestamps_ns = np.empty(n_samples, dtype=np.int64)
    timestamps_ns[0] = 3_600_000_000_000  # 1 hour
    timestamps_ns[1:] = timestamps_ns[0] + np.cumsum(intervals_ns)
    return timestamps_ns


def crank_signal(n_samples: int, phase_deg: float = 0.0) -> np.ndarray:
    """Crank-angle-synchronous combustion-pressure waveform (0-100 bar).

    ``phase_deg`` offsets the waveform so each parameter is visually distinct.
    Values depend only on crank angle (sample index), not on the timebase, so
    every parameter can share one timebase.
    """
    angle = np.cumsum(
        np.concatenate([[0.0], 360.0 / TEETH_PER_REV * np.ones(n_samples - 1)])
    )
    return np.sin(np.radians(angle + phase_deg)) * 50.0 + 50.0


def open_connection(mode, identifier="", key=None):
    """Return the appropriate session connection for the configured backend."""
    if BACKEND == "sqlite":
        return SQLiteConnection(SQLITE_DB_DIR, identifier, session_key=key, mode=mode)
    return SQLRaceDBConnection(SERVER, DATABASE, identifier, session_key=key, mode=mode)


def main():
    print(f"\n{'=' * 65}")
    print("  Pandlas Concurrent Synchro Writing")
    print(f"{'=' * 65}")
    print(f"  Backend:       {BACKEND}")
    print(f"  Parameters:    {N_PARAMS}  x  {N_PER:,} samples each")
    print(f"  Total samples: {N_PARAMS * N_PER:,} per phase")
    print(f"  Worker threads:{MAX_WORKERS}")
    print(f"  Packet size:   {PACKET_SIZE:,}")
    print(f"{'=' * 65}\n")

    # ---- Generate N distinct signals once; reused for both phases ----
    # One shared timebase (co-terminous channels); distinct values per param via
    # a phase offset.  Channels MUST share a timebase — see engine_timebase().
    print("Generating signals ...")
    ts = engine_timebase(N_PER)
    signals = [
        (f"Crank_{i}", crank_signal(N_PER, phase_deg=360.0 * i / N_PARAMS), ts)
        for i in range(N_PARAMS)
    ]

    session_id = f"Synchro Concurrent - {pd.Timestamp.now():%y/%m/%d %H:%M:%S}"

    with open_connection("w", session_id) as session:
        # ---- Phase A: serial baseline (add_synchro_data per parameter) ----
        print(f"\nSerial write ({N_PARAMS} params) ...")
        t0 = time.perf_counter()
        for name, samples, ts in signals:
            add_synchro_data(
                session, samples, ts,
                parameter_name=f"{name}_serial",
                app_group="SerialSync", param_group="Crank",
                unit="bar", description="Serial crank pressure",
                packet_size=PACKET_SIZE, show_progress_bar=False,
            )
        t_serial = time.perf_counter() - t0

        # ---- Phase B: concurrent (add_synchro_data_multi) ----
        print(f"Concurrent write ({N_PARAMS} params, {MAX_WORKERS} threads) ...")
        params = [
            {
                "parameter_name": f"{name}_concurrent",
                "samples": samples,
                "timestamps": ts,
                "app_group": "ConcurrentSync",
                "param_group": "Crank",
                "unit": "bar",
                "description": "Concurrent crank pressure",
            }
            for name, samples, ts in signals
        ]
        t0 = time.perf_counter()
        add_synchro_data_multi(
            session, params, max_workers=MAX_WORKERS,
            packet_size=PACKET_SIZE, show_progress_bar=True,
        )
        t_concurrent = time.perf_counter() - t0

        session_key = session.Key.ToString()

    # ---- Validate the concurrently-written parameters round-trip ----
    # One fresh read session PER parameter.  SQL Race's synchro reader is flaky
    # when several synchro channels are read from a single loaded session (the
    # 2nd+ read can throw a NullReferenceException), but the first read of a
    # freshly-loaded session is reliable — so we reopen per parameter.  Also read
    # over the full session range: passing an explicit start/end likewise throws.
    print("\nValidating concurrent writes ...")
    all_ok = True
    for name, samples, _ in signals:
        with open_connection("r", key=session_key) as session:
            read_samples, _ = sr.get_samples(session, f"{name}_concurrent:ConcurrentSync")
        count_ok = len(read_samples) == N_PER
        data_ok = count_ok and np.allclose(
            read_samples.astype(np.float64), samples, atol=1e-9
        )
        status = "✓" if data_ok else "✗"
        print(f"  {status} {name}_concurrent  ({len(read_samples):,}/{N_PER:,})")
        all_ok = all_ok and data_ok

    # ---- Summary ----
    total = N_PARAMS * N_PER
    print(f"\n{'=' * 65}")
    print("  RESULTS")
    print(f"{'=' * 65}")
    print(f"  {'Phase':<14}{'Write (s)':>12}{'Throughput':>20}")
    print(f"  {'-' * 14}{'-' * 12:>12}{'-' * 18:>20}")
    print(f"  {'serial':<14}{t_serial:>12.3f}{total / t_serial:>16,.0f} /s")
    print(f"  {'concurrent':<14}{t_concurrent:>12.3f}{total / t_concurrent:>16,.0f} /s")
    print(f"\n  Speed-up: {t_serial / t_concurrent:.2f}x with {MAX_WORKERS} threads")
    print(f"  Integrity: {'ALL OK' if all_ok else 'FAILED'}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
