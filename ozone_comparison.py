from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Sequence

import yaml

# Adjust this import to your actual package structure, e.g.:
# from mkndaq.inst.thermo import Thermo49C
from mkndaq.inst.thermo import Thermo49C

logger = logging.getLogger(__name__)


class Thermo49CPS(Thermo49C):
    """Thermo 49C-PS ozone calibrator helper.

    Extends Thermo49C with:
    - an O3 setpoint (in ppb)
    - an extra column "setpoint_ppb" appended to each lrec line.
    """

    def __init__(self, name: str, config: dict) -> None:
        super().__init__(name, config)
        cal_cfg = config.get(name, {})

        # Sequence of levels (ppb) and dwell time per level (minutes).
        self.levels: list[int] = [int(x) for x in cal_cfg.get("levels", [])]
        self.level_duration_minutes: int = int(cal_cfg.get("duration_minutes", 15))

        # Template for the remote command that sets the level.
        # The value (in ppb) is passed in as {value}.
        # -> adapt this to the real TEI49C-PS syntax if needed.
        self.level_cmd_template: str = cal_cfg.get(
            "level_cmd_template", "set o3 conc {value}"
        )

        # Track the last requested level (ppb).
        self.current_level: float | None = None

        # Make sure the header has a column for the setpoint.
        if "setpoint" not in self._data_header:
            self._data_header = f"{self._data_header} setpoint_ppb"

    # ------------------------------------------------------------------
    # High-level API
    # ------------------------------------------------------------------
    def set_o3_level(self, level_ppb: int) -> str:
        """Set the calibrator ozone concentration (in ppb).

        The actual command string is controlled by ``level_cmd_template``.
        By default this sends e.g. "set o3 conc 80" for 80 ppb.
        """
        cmd = self.level_cmd_template.format(value=int(level_ppb))
        reply = self.serial_comm(cmd)  # same helper as in Thermo49C
        self.current_level = float(level_ppb)
        self.logger.info(
            "[%s] set_o3_level -> %d ppb (cmd=%r, reply=%r)",
            self.name,
            self.current_level,
            cmd,
            reply,
        )
        return reply

    def accumulate_lrec(self) -> None:
        """Collect one lrec and append it to the internal buffer.

        Compared to Thermo49C.accumulate_lrec(), this version appends the
        current O3 setpoint (in ppb) as the last column on each line.
        """
        # Respect any cool-down set by failing I/O (same pattern as base class).
        if getattr(self, "_cooldown_until", 0.0) and time.time() < self._cooldown_until:
            return

        try:
            dtm = time.strftime("%Y-%m-%d %H:%M:%S")
            lrec = self.serial_comm("lrec")
            if not lrec:
                return

            # Append the current setpoint; empty if none has been set yet.
            setpoint_str = (
                "" if self.current_level is None else f" {self.current_level}"
            )
            self._data += f"{dtm} {lrec}{setpoint_str}\n"
            self.logger.debug(
                "[%s] lrec: %s ... (setpoint=%s)",
                self.name,
                lrec[:60],
                self.current_level,
            )
        except Exception as err:  # defensive
            self.logger.error("[%s] accumulate_lrec: %s", self.name, err)


def set_o3_level(calibrator: Thermo49CPS, level_ppb: int) -> str:
    """Free-function wrapper for convenience."""
    return calibrator.set_o3_level(level_ppb)


def load_config(config_path: Path) -> dict:
    with config_path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def run_calibration(
    config_path: Path,
    cal_name: str = "tei49c-ps",
    ana_name: str = "tei49c",
) -> None:
    """Run a full calibration sequence using mkndaq.yml.

    - Reads `levels` and `duration_minutes` from `cal_name` (tei49c-ps).
    - For each level:
        * calls set_o3_level()
        * repeatedly calls accumulate_lrec() on both calibrator and analyzer
          with the usual lrec format.
    - At the end, flushes data via the normal _save_and_stage_data() helpers.
    """
    cfg = load_config(config_path)

    calibrator = Thermo49CPS(cal_name, cfg)
    analyzer = Thermo49C(ana_name, cfg)

    # Reuse existing helpers to create directories and timestamp format.
    # (setup_schedules sets _file_timestamp_format etc.)
    calibrator.setup_schedules()
    analyzer.setup_schedules()

    levels: Sequence[int] = calibrator.levels
    if not levels:
        logger.error("No 'levels' configured for %s in %s", cal_name, config_path)
        return

    duration_min = calibrator.level_duration_minutes
    sample_seconds = int(calibrator.sampling_interval) * 60

    logger.info(
        "Starting ozone calibration: %d levels, %d min/level, sampling every %d s",
        len(levels),
        duration_min,
        sample_seconds,
    )

    for level in levels:
        calibrator.set_o3_level(level)
        logger.info("Holding %d ppb for %d minutes", level, duration_min)

        end = time.time() + duration_min * 60
        while time.time() < end:
            # Existing accumulate_lrec() on each instrument
            calibrator.accumulate_lrec()
            analyzer.accumulate_lrec()
            time.sleep(sample_seconds)

    # Flush remaining data to disk and stage ZIP archives via the existing logic.
    calibrator._save_and_stage_data()
    analyzer._save_and_stage_data()

    logger.info("Ozone calibration sequence finished.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run TEI49C-PS ozone calibration sequence."
    )
    parser.add_argument(
        "-c",
        "--config",
        type=Path,
        default=Path("mkndaq.yml"),
        help="Path to mkndaq.yml configuration file.",
    )
    parser.add_argument(
        "--cal-name",
        default="tei49c-ps",
        help="Config section name for the calibrator (default: tei49c-ps).",
    )
    parser.add_argument(
        "--ana-name",
        default="tei49c",
        help="Config section name for the analyzer under test (default: tei49c).",
    )
    args = parser.parse_args()
    run_calibration(args.config, cal_name=args.cal_name, ana_name=args.ana_name)


if __name__ == "__main__":
    main()
