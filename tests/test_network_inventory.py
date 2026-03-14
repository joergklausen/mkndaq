from __future__ import annotations

import concurrent.futures as cf
from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
from collections import defaultdict

import pytest
import yaml

pytestmark = [pytest.mark.integration]

HOST_KEYS = ("host", "ip", "ip_address", "address", "hostname")
NESTED_HOST_CONTAINERS = ("socket", "tcp", "net", "network")
DEFAULT_CONFIG_CANDIDATES = (
    Path("dist/mkndaq.yml"),
    Path("mkndaq.yml"),
    Path("configs/mkndaq.yml"),
)


@dataclass(frozen=True, slots=True)
class Target:
    name: str
    ip: str
    port: int | None = None


def _find_config() -> Path:
    env_path = os.getenv("MKNDAQ_CONFIG")
    if env_path:
        path = Path(env_path).expanduser()
        if path.exists():
            return path
        raise FileNotFoundError(f"MKNDAQ_CONFIG does not exist: {path}")

    here = Path(__file__).resolve()
    for base in (here.parent,) + tuple(here.parents):
        for candidate in DEFAULT_CONFIG_CANDIDATES:
            path = base / candidate
            if path.exists():
                return path

    searched = ", ".join(str(p) for p in DEFAULT_CONFIG_CANDIDATES)
    raise FileNotFoundError(f"Could not find mkndaq config. Searched for: {searched}")


def _load_yaml(path: Path) -> dict:
    text = path.read_text(encoding="utf-8-sig")
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise TypeError(f"Top-level YAML must be a dict, got {type(data).__name__}")
    return data


def _is_instrument_section(value: object) -> bool:
    return isinstance(value, dict) and "type" in value


def _split_host_value(value: object) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            out.extend(_split_host_value(item))
        return out
    return [str(value).strip()]


def _parse_lan_ip(text: str) -> str | None:
    try:
        ip = ipaddress.ip_address(text)
    except ValueError:
        return None

    if ip.is_unspecified:
        return None
    if ip.version != 4:
        return None
    if not (ip.is_private or ip.is_link_local):
        return None
    return str(ip)



def _extract_targets(cfg: dict) -> list[Target]:
    targets: list[Target] = []

    for name, section in cfg.items():
        if not _is_instrument_section(section):
            continue

        port: int | None = None
        if isinstance(section.get("socket"), dict):
            raw_port = section["socket"].get("port")
            if isinstance(raw_port, int):
                port = raw_port
            elif isinstance(raw_port, str) and raw_port.strip().isdigit():
                port = int(raw_port.strip())

        raw_hosts: list[str] = []
        for container in NESTED_HOST_CONTAINERS:
            nested = section.get(container)
            if isinstance(nested, dict):
                for key in HOST_KEYS:
                    raw_hosts.extend(_split_host_value(nested.get(key)))

        for key in HOST_KEYS:
            raw_hosts.extend(_split_host_value(section.get(key)))

        for raw_host in raw_hosts:
            lan_ip = _parse_lan_ip(raw_host)
            if lan_ip:
                targets.append(Target(name=str(name), ip=lan_ip, port=port))

    deduped: dict[tuple[str, str, int | None], Target] = {}
    for target in targets:
        deduped[(target.name, target.ip, target.port)] = target
    return sorted(deduped.values(), key=lambda t: (ipaddress.ip_address(t.ip), t.name, t.port or -1))



def _as_ipv4_network(value: str) -> ipaddress.IPv4Network:
    network = ipaddress.ip_network(value, strict=False)
    if not isinstance(network, ipaddress.IPv4Network):
        raise ValueError(f"Expected an IPv4 network, got: {value}")
    return network


def _derive_networks(configured_ips: set[str]) -> list[ipaddress.IPv4Network]:
    override = os.getenv("MKNDAQ_SCAN_NETWORKS", "").strip()
    if override:
        networks: list[ipaddress.IPv4Network] = []
        for item in override.split(","):
            item = item.strip()
            if item:
                networks.append(_as_ipv4_network(item))
        return sorted(set(networks), key=lambda n: (int(n.network_address), n.prefixlen))

    network_set: set[ipaddress.IPv4Network] = {
        _as_ipv4_network(f"{ip}/24")
        for ip in configured_ips
    }
    return sorted(network_set, key=lambda n: int(n.network_address))


def _ping_command(ip: str, timeout_ms: int) -> list[str]:
    system = platform.system().lower()
    if system == "windows":
        return ["ping", "-n", "1", "-w", str(timeout_ms), ip]
    return ["ping", "-c", "1", "-W", str(max(1, timeout_ms // 1000)), ip]



def _ping(ip: str, timeout_ms: int = 400) -> bool:
    if shutil.which("ping") is None:
        raise RuntimeError("The 'ping' command is not available on PATH.")

    result = subprocess.run(
        _ping_command(ip, timeout_ms=timeout_ms),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0



def _tcp_probe(ip: str, port: int, timeout: float = 0.4) -> bool:
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            return True
    except OSError:
        return False



def _scan_networks(
    networks: list[ipaddress.IPv4Network],
    *,
    timeout_ms: int,
    max_workers: int,
) -> set[str]:
    candidates = [str(host) for network in networks for host in network.hosts()]
    reachable: set[str] = set()

    with cf.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_ping, ip, timeout_ms): ip for ip in candidates}
        for future in cf.as_completed(futures):
            ip = futures[future]
            if future.result():
                reachable.add(ip)

    return reachable



def _env_truthy(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}



def _format_ip_report(title: str, ips: set[str], labels_by_ip: dict[str, list[str]] | None = None) -> list[str]:
    lines = [title]
    for ip in sorted(ips, key=lambda s: tuple(int(part) for part in s.split("."))):
        if labels_by_ip and ip in labels_by_ip:
            labels = ", ".join(sorted(labels_by_ip[ip]))
            lines.append(f"  - {ip}  ({labels})")
        else:
            lines.append(f"  - {ip}")
    return lines



def test_configured_lan_inventory_matches_network_scan() -> None:
    if not _env_truthy("MKNDAQ_RUN_NETWORK_TESTS"):
        pytest.skip("Set MKNDAQ_RUN_NETWORK_TESTS=1 to enable LAN integration scanning.")

    config_path = _find_config()
    cfg = _load_yaml(config_path)
    targets = _extract_targets(cfg)

    assert targets, f"No LAN targets found in config: {config_path}"

    configured_ips = {target.ip for target in targets}
    labels_by_ip: dict[str, list[str]] = defaultdict(list)
    ports_by_ip: dict[str, set[int]] = defaultdict(set)
    for target in targets:
        labels_by_ip[target.ip].append(target.name)
        if target.port is not None:
            ports_by_ip[target.ip].add(target.port)

    networks = _derive_networks(configured_ips)
    max_workers = int(os.getenv("MKNDAQ_SCAN_WORKERS", "128"))
    ping_timeout_ms = int(os.getenv("MKNDAQ_PING_TIMEOUT_MS", "300"))
    tcp_timeout_s = float(os.getenv("MKNDAQ_TCP_TIMEOUT_S", "0.4"))

    reachable_ips = _scan_networks(
        networks,
        timeout_ms=ping_timeout_ms,
        max_workers=max_workers,
    )

    # Fallback: some configured devices may block ICMP but still accept TCP.
    for ip, ports in ports_by_ip.items():
        if ip in reachable_ips:
            continue
        if any(_tcp_probe(ip, port, timeout=tcp_timeout_s) for port in sorted(ports)):
            reachable_ips.add(ip)

    ignored_ips = {
        _parse_lan_ip(item.strip())
        for item in os.getenv("MKNDAQ_SCAN_IGNORE", "").split(",")
        if item.strip()
    }
    ignored_ips.discard(None)
    ignored_ips = {ip for ip in ignored_ips if ip is not None}

    configured_but_unreachable = configured_ips - reachable_ips
    reachable_but_unconfigured = reachable_ips - configured_ips - ignored_ips

    if configured_but_unreachable or reachable_but_unconfigured:
        report: list[str] = [
            f"LAN inventory mismatch for config: {config_path}",
            "",
            "Scanned networks:",
            *[f"  - {network}" for network in networks],
            "",
            f"Configured LAN IPs: {len(configured_ips)}",
            f"Reachable LAN IPs found by scan: {len(reachable_ips)}",
            "",
        ]

        if configured_but_unreachable:
            report.extend(
                _format_ip_report(
                    "Configured in YAML but not reachable on the LAN:",
                    configured_but_unreachable,
                    labels_by_ip,
                )
            )
            report.append("")

        if reachable_but_unconfigured:
            report.extend(
                _format_ip_report(
                    "Reachable on the LAN but not mentioned in the YAML:",
                    reachable_but_unconfigured,
                )
            )
            report.append("")

        if ignored_ips:
            report.extend(_format_ip_report("Ignored IPs:", ignored_ips))
            report.append("")

        pytest.fail("\n".join(report))
