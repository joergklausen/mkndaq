# -*- coding: utf-8 -*-
"""
mkndaq.inst.meteo

Fetch new MeteoSwiss meteo bulletins (VRXA0*) from a linuxbox on the same LAN
via key-based SSH/SFTP, then store and stage them like other instruments.

Local outputs:
  - Archive:  <root>/<data>/<meteo.data_path>/YYYY/MM/DD/VRXA00...
  - Staging:  <root>/<staging>/<meteo.staging_path>/VRXA00...   (or .zip if staging_zip=True)

The staging directory is what mkndaq.py uses for scheduled transfers.

@author: joerg.klausen@meteoswiss.ch
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

import colorama

from mkndaq.utils.sftp import SFTPClient


class METEO:
    """
    Virtual MeteoSwiss bulletin instrumentation.

    The linuxbox produces bulletin files (VRXA00...) under a remote directory.
    This class fetches new files via SFTP (SSH, key-based) and stages them for transfer.
    """

    def __init__(self, name: str, config: dict) -> None:
        colorama.init(autoreset=True)

        self.name = name
        self.config = config

        _logger = f"{os.path.basename(config['logging']['file'])}".split('.')[0]
        self.logger = logging.getLogger(f"{_logger}.{__name__}")
        self.logger.info(f"[{self.name}] Initializing")

        inst_cfg = config.get(name, {})

        socket = inst_cfg.get("socket", {}) or {}

        host = socket.get("host")
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"[{self.name}] missing/invalid config: {name}.socket.host")
        self.host: str = host.strip()

        port = socket.get("port", 22)
        try:
            self.port: int = int(port)
        except Exception as err:
            raise ValueError(f"[{self.name}] invalid config: {name}.socket.port={port!r}") from err

        usr = socket.get("usr") or socket.get("user") or inst_cfg.get("usr") or "admin"
        if not isinstance(usr, str) or not usr.strip():
            raise ValueError(f"[{self.name}] invalid config: {name}.socket.usr")
        self.usr: str = usr.strip()

        # remote directory and file filter
        self.source: str = str(inst_cfg.get("source", "/"))
        self.pattern: str = str(inst_cfg.get("pattern", "VRXA00"))

        # SSH key path (private key)
        self.key_path = Path(str(inst_cfg.get("key", ""))).expanduser()
        if not str(self.key_path):
            raise ValueError(f"[{self.name}] missing config: {name}.key (path to private key)")
        if not self.key_path.exists():
            raise FileNotFoundError(f"[{self.name}] SSH private key not found: {self.key_path}")

        # setup local paths
        root = Path(os.path.expanduser(str(config["root"])))
        self.data_path = (root / str(config["data"]) / str(inst_cfg.get("data_path", name))).resolve()
        self.staging_path = (root / str(config["staging"]) / str(inst_cfg.get("staging_path", name))).resolve()
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.staging_path.mkdir(parents=True, exist_ok=True)

        # reporting interval (minutes)
        self.reporting_interval: int = int(inst_cfg.get("reporting_interval", 10))

        # staging options
        self._zip: bool = bool(inst_cfg.get("staging_zip", False))

        # remote transfer prefix used by mkndaq.py transfers
        self.remote_path = str(inst_cfg.get("remote_path", name))

        # incremental state to prevent re-downloading
        self._state_file = self.data_path / ".meteo_fetch_state.json"

        # Reuse mkndaq.utils.sftp.SFTPClient for key loading + SFTP session handling.
        # Override host/usr/key/port because this is NOT the MeteoSwiss SFTP endpoint.
        self.lan_sftp = SFTPClient(
            config=config,
            host=self.host,
            usr=self.usr,
            key_path=self.key_path,
            port=self.port,
        )

        if "usr" not in socket and "user" not in socket and "usr" not in inst_cfg:
            self.logger.warning(
                f"[{self.name}] No SSH username configured (meteo.socket.usr). Using default '{self.usr}'."
            )

    def _read_state(self) -> Set[str]:
        try:
            if not self._state_file.exists():
                return set()
            raw = json.loads(self._state_file.read_text(encoding="utf-8"))
            return set(str(x) for x in raw.get("seen", []))
        except Exception as err:
            self.logger.warning(f"[{self.name}] Could not read state file: {err}")
            return set()

    def _write_state(self, seen: Set[str]) -> None:
        try:
            N = 20000
            payload = {"seen": sorted(list(seen))[-N:]}
            tmp = self._state_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self._state_file)
        except Exception as err:
            self.logger.warning(f"[{self.name}] Could not write state file: {err}")

    def store_and_stage_files(self) -> None:
        """Entry point used by mkndaq.py (fetch → archive → stage)."""
        try:
            new_files = self.fetch_new_bulletins()
            if new_files:
                self.logger.info(f"[{self.name}] fetched {len(new_files)} new bulletin file(s)")
        except Exception as err:
            self.logger.error(f"[{self.name}] store_and_stage_files: {err}")

    def fetch_new_bulletins(self) -> list[Path]:
        """
        Fetch bulletin files matching self.pattern from remote linuxbox into staging_path,
        copy to /data archive partitioned by remote mtime, keep (or zip) in staging.

        Returns:
            list[Path]: staged file paths (raw or zipped)
        """
        seen = self._read_state()

        downloaded = self.lan_sftp.download_files(
            remote_dir=self.source,
            local_dir=self.staging_path,
            pattern=self.pattern,
            skip_names=seen,
        )

        staged: list[Path] = []

        for d in downloaded:
            # archive copy into /data partitioned by mtime
            dt = datetime.fromtimestamp(d.mtime)
            archive_dir = self.data_path / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
            archive_dir.mkdir(parents=True, exist_ok=True)
            shutil.copy2(d.local_path, archive_dir / d.name)

            # optionally zip in staging (raw stays in archive)
            if self._zip:
                zip_path = self.staging_path / f"{Path(d.name).stem}.zip"
                with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                    zf.write(d.local_path, arcname=d.local_path.name)
                d.local_path.unlink(missing_ok=True)
                staged.append(zip_path)
            else:
                staged.append(d.local_path)

            seen.add(d.name)

        if downloaded:
            self._write_state(seen)

        return staged

    def print_meteo(self) -> None:
        """Log the latest decoded bulletin at DEBUG level."""
        data = {}
        try:
            bulletin = self._latest_local_bulletin()
            if bulletin is None:
                self.logger.warning(colorama.Fore.RED + f"[{self.name}] no recent data to display")
                return
            data = self.extract_bulletin(bulletin)
            self.logger.debug(
                colorama.Fore.GREEN
                + f"[{self.name}] "
                + ", ".join([f"{k}: {v}" for k, v in data.items()])
            )
        except Exception as err:
            self.logger.error(colorama.Fore.RED + f"[{self.name}] print_meteo: {err}; data: {data}")

    def _latest_local_bulletin(self) -> Optional[Path]:
        # Prefer archive (always raw); staging may contain only zips if staging_zip=True
        candidates = [p for p in self.data_path.rglob(f"{self.pattern}*") if p.is_file()]
        if not candidates:
            candidates = [p for p in self.staging_path.iterdir() if p.is_file() and self.pattern in p.name]
        if not candidates:
            return None
        return max(candidates, key=lambda p: p.stat().st_mtime)

    def extract_bulletin(self, file: Path | str) -> dict:
        """Read a VRXA00 bulletin file and extract the header/data dict."""
        try:
            file = Path(file)
            if "VRXA00" in file.name:
                with file.open("r", encoding="utf-8") as fh:
                    for _ in range(3):
                        fh.readline()
                    header = fh.readline().split()
                    data = fh.readline().split()
                return dict(zip(header, data))
            return {}
        except Exception as err:
            self.logger.error(err)
            return {}
