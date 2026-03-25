#!/usr/bin/env python3

import csv
import logging
import math
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class Config:
    output_dir: Path
    source_regex: re.Pattern[str]
    obs_margin: float
    min_sleep_seconds: int
    max_sleep_seconds: int
    chrony_socket: Path
    node_name: str
    state_file: Path
    log_file: Path


@dataclass
class RawOffsetSample:
    source: str
    raw_offset: str
    adjusted_offset: str
    error_bound: str
    observed_at_local: str
    observed_at_epoch_s: int
    node_name: str
    source_line: str
    sleep_seconds: int


def build_config() -> Config:
    output_dir = Path(os.getenv("OUTPUT_DIR", "/data"))
    output_dir.mkdir(parents=True, exist_ok=True)

    node_name = os.getenv("NODE_NAME", socket.gethostname())
    chrony_socket = Path(os.getenv("CHRONY_SOCKET", "/var/run/chrony/chronyd.sock"))

    source_pattern = os.getenv(
        "SOURCE_REGEX",
        r"metadata\.google\.internal|169\.254\.169\.254",
    )

    obs_margin = float(os.getenv("OBS_MARGIN", "1.10"))
    min_sleep_seconds = int(os.getenv("MIN_SLEEP_SECONDS", "1"))
    max_sleep_seconds = int(os.getenv("MAX_SLEEP_SECONDS", "300"))

    return Config(
        output_dir=output_dir,
        source_regex=re.compile(source_pattern),
        obs_margin=obs_margin,
        min_sleep_seconds=min_sleep_seconds,
        max_sleep_seconds=max_sleep_seconds,
        chrony_socket=chrony_socket,
        node_name=node_name,
        state_file=output_dir / f"state-{node_name}.txt",
        log_file=output_dir / f"raw-offset-{node_name}.csv",
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S%z",
    )


def run_chronyc(config: Config, *args: str) -> str:
    cmd = ["chronyc", "-h", str(config.chrony_socket), *args]
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"chronyc command failed: {' '.join(cmd)} | stderr={result.stderr.strip()}"
        )
    return result.stdout


def check_requirements(config: Config) -> None:
    if shutil.which("chronyc") is None:
        raise RuntimeError("chronyc is not installed in the container")

    if not config.chrony_socket.exists():
        raise RuntimeError(f"chronyd socket path does not exist: {config.chrony_socket}")

    if not config.chrony_socket.is_socket():
        raise RuntimeError(f"chronyd socket is not a socket: {config.chrony_socket}")

    _ = run_chronyc(config, "tracking")
    logging.info("chronyd is reachable through %s", config.chrony_socket)


def parse_update_interval_seconds(tracking_output: str) -> float:
    match = re.search(r"^Update interval\s*:\s*([0-9]+(?:\.[0-9]+)?)\s+seconds", tracking_output, re.MULTILINE)
    if not match:
        raise RuntimeError("unable to parse Update interval from chronyc tracking output")
    return float(match.group(1))


def get_sleep_seconds(config: Config) -> int:
    tracking_output = run_chronyc(config, "tracking")
    update_interval_s = parse_update_interval_seconds(tracking_output)

    sleep_seconds = math.ceil(update_interval_s * config.obs_margin)
    sleep_seconds = max(config.min_sleep_seconds, sleep_seconds)
    sleep_seconds = min(config.max_sleep_seconds, sleep_seconds)

    return sleep_seconds


def find_source_line(config: Config, sources_output: str) -> Optional[str]:
    for line in sources_output.splitlines():
        if config.source_regex.search(line):
            return line
    return None


def parse_source_line(line: str) -> tuple[str, str, str, str]:
    """
    Parse a line from `chronyc sources -v`.

    Expected tail shape is typically like:
      ...  3us[  7us] +/-  12ms

    Interpretation:
    - adjusted_offset: value outside brackets
    - raw_offset: value inside brackets (raw observed sample)
    - error_bound: value after +/-.
    """
    parts = line.split()
    source = parts[1] if len(parts) > 1 else "unknown"

    match = re.search(
        r"([+-]?\s*\d+(?:\.\d+)?\s*(?:ns|us|ms|s))\s*"
        r"\[\s*([+-]?\s*\d+(?:\.\d+)?\s*(?:ns|us|ms|s))\s*\]\s*"
        r"\+/-\s*([+-]?\s*\d+(?:\.\d+)?\s*(?:ns|us|ms|s))",
        line,
    )
    if not match:
        raise RuntimeError(f"unable to parse offset values from sources line: {line}")

    adjusted_offset = re.sub(r"\s+", "", match.group(1))
    raw_offset = re.sub(r"\s+", "", match.group(2))
    error_bound = re.sub(r"\s+", "", match.group(3))

    return source, raw_offset, adjusted_offset, error_bound


def read_raw_offset_sample(config: Config, sleep_seconds: int) -> RawOffsetSample:
    sources_output = run_chronyc(config, "sources", "-v")
    line = find_source_line(config, sources_output)
    if line is None:
        raise RuntimeError(
            f"source line not found for pattern: {config.source_regex.pattern}"
        )

    source, raw_offset, adjusted_offset, error_bound = parse_source_line(line)
    now = datetime.now().astimezone()

    return RawOffsetSample(
        source=source,
        raw_offset=raw_offset,
        adjusted_offset=adjusted_offset,
        error_bound=error_bound,
        observed_at_local=now.strftime("%Y-%m-%d %H:%M:%S%z"),
        observed_at_epoch_s=int(now.timestamp()),
        node_name=config.node_name,
        source_line=line,
        sleep_seconds=sleep_seconds,
    )


def ensure_csv_header(config: Config) -> None:
    if config.log_file.exists():
        return

    with config.log_file.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "local_time",
                "epoch_s",
                "node",
                "source",
                "raw_offset",
                "adjusted_offset",
                "error_bound",
            ]
        )


def append_sample(config: Config, sample: RawOffsetSample) -> None:
    with config.log_file.open("a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                sample.observed_at_local,
                sample.observed_at_epoch_s,
                sample.node_name,
                sample.source,
                sample.raw_offset,
                sample.adjusted_offset,
                sample.error_bound,
            ]
        )


def write_state(config: Config, sample: RawOffsetSample) -> None:
    config.state_file.write_text(
        "\n".join(
            [
                f"last_sleep_seconds={sample.sleep_seconds}",
                f"last_line={sample.source_line}",
            ]
        )
        + "\n"
    )


def main() -> int:
    configure_logging()
    config = build_config()

    try:
        check_requirements(config)
        ensure_csv_header(config)

        while True:
            try:
                sleep_seconds = get_sleep_seconds(config)
                sample = read_raw_offset_sample(config, sleep_seconds)
                append_sample(config, sample)
                write_state(config, sample)

                logging.info(
                    "node=%s source=%s raw_offset=%s adjusted_offset=%s error_bound=%s next_sleep=%ss",
                    sample.node_name,
                    sample.source,
                    sample.raw_offset,
                    sample.adjusted_offset,
                    sample.error_bound,
                    sample.sleep_seconds,
                )
            except Exception as exc:
                logging.exception("measurement failed: %s", exc)
                sleep_seconds = max(config.min_sleep_seconds, 5)

            time.sleep(sleep_seconds)

    except Exception as exc:
        logging.exception("collector startup failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())