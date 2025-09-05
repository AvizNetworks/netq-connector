# collector/inventory/interface.py
#!/usr/bin/env python3
import os
import re
import json
import logging
from typing import Dict, List, Optional, Tuple
import requests
import urllib3
from kafka import KafkaProducer

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("collector.inventory.interface")


class InterfaceCollector:
    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
        login_url: str,
        interface_url: str,
        username: str,
        password: str,
        connector_name: str,
        connector_type: str,
        connector_tags: str,
        inventory_url: str,
    ):
        self.producer = producer
        self.topic = topic

        self.login_url = login_url
        self.interface_url = interface_url
        self.inventory_url = inventory_url
        self.username = username
        self.password = password

        self.connector_name = connector_name
        self.connector_type = connector_type
        self.connector_tags = connector_tags


        self._base = login_url.split("/api/")[0].rstrip("/")

        self.addr_url = f"{self._base}/api/netq/telemetry/v1/object/address"
        self.link_url = f"{self._base}/api/netq/telemetry/v1/object/link"
        self.port_url = f"{self._base}/api/netq/telemetry/v1/object/port"
        self.procdev_url = f"{self._base}/api/netq/telemetry/v1/object/procdevstats"

    # ----------------- Auth -----------------
    def _get_token(self) -> Optional[str]:
        try:
            r = requests.post(
                self.login_url,
                json={"username": self.username, "password": self.password},
                verify=False,
                timeout=15,
            )
            r.raise_for_status()
            return r.json().get("access_token")
        except Exception as e:
            log.error("[iface] login failed: %s", e)
            return None


    def _fetch_json_list(self, url: str, token: str, what: str) -> List[Dict]:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers, verify=False, timeout=30)
            r.raise_for_status()
            body = r.json()
            return body if isinstance(body, list) else body.get("data", [])
        except Exception as e:
            log.warning("[iface] fetch %s failed: %s", what, e)
            return []

    def _fetch_interfaces(self, token: str) -> List[Dict]:
        return self._fetch_json_list(self.interface_url, token, "/object/interface")

    def _fetch_inventory_mac_map(self, token: str) -> Dict[str, str]:
        rows = self._fetch_json_list(self.inventory_url, token, "/object/inventory")
        out: Dict[str, str] = {}
        for row in rows:
            host = (row.get("hostname") or "").lower()
            mac = (row.get("platform_base_mac") or "").strip()
            if host and mac:
                out[host] = mac
        return out

    def _fetch_address_map(self, token: str) -> Dict[Tuple[str, str], str]:
        """Return {(hostname_lower, ifname): ip_prefix} from /object/address"""
        rows = self._fetch_json_list(self.addr_url, token, "/object/address")
        out: Dict[Tuple[str, str], str] = {}
        for row in rows:
            host = (row.get("hostname") or "").lower()
            ifname = row.get("ifname") or ""
            ip = row.get("prefix") or ""
            if host and ifname and ip:
                out[(host, ifname)] = ip
        return out

    def _fetch_links_map(self, token: str) -> Dict[Tuple[str, str], Dict]:
        """Return {(hostname_lower, ifname): link_row} from /object/link (handles both shapes)"""
        rows = self._fetch_json_list(self.link_url, token, "/object/link")
        out: Dict[Tuple[str, str], Dict] = {}
        for row in rows:
            host = (row.get("hostname") or row.get("name") or "").lower()
            if "links" in row and isinstance(row["links"], list):
                for l in row["links"]:
                    ifname = l.get("ifname") or l.get("name")
                    if host and ifname:
                        out[(host, ifname)] = l
            else:
                ifname = row.get("ifname") or row.get("name")
                if host and ifname:
                    out[(host, ifname)] = row
        return out

    def _fetch_port_map(self, token: str) -> Dict[Tuple[str, str], Dict]:
        """Return {(hostname_lower, ifname): port_row} from /object/port"""
        rows = self._fetch_json_list(self.port_url, token, "/object/port")
        out: Dict[Tuple[str, str], Dict] = {}
        for row in rows:
            host = (row.get("hostname") or row.get("name") or "").lower()
            ifname = row.get("ifname") or row.get("name")
            if host and ifname:
                out[(host, ifname)] = row
        return out

    def _fetch_procdev_map(self, token: str) -> Dict[Tuple[str, str], Dict]:
        rows = self._fetch_json_list(self.procdev_url, token, "/object/procdevstats")
        out: Dict[Tuple[str, str], Dict] = {}
        for row in rows:
            host = (row.get("hostname") or "").lower()
            ifname = row.get("ifname") or ""
            if host and ifname:
                out[(host, ifname)] = row
        return out


    _mtu_re = re.compile(r"\bMTU:\s*(\d+)\b", re.IGNORECASE)
    _speed_re = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([KMG]?)\s*[bB]?(?:ps)?\s*$")

    @staticmethod
    def _first_nonempty(*vals) -> str:
        for v in vals:
            if isinstance(v, (int, float)):
                return str(int(v))
            if isinstance(v, str) and v.strip() != "":
                return v
        return ""

    def _parse_mtu_from_text(self, text: str) -> str:
        if not text:
            return ""
        m = self._mtu_re.search(text)
        return m.group(1) if m else ""

    def _speed_to_mbps_int(self, val: str) -> str:

        if not val or not isinstance(val, str):
            return ""
        s = val.strip()
        if not s or s.lower() == "unknown":
            return ""
        m = self._speed_re.match(s)
        if not m:
            # quick path for forms like "1G"
            unit = s[-1].upper() if s[-1].isalpha() else ""
            num_part = s[:-1] if unit else s
            if num_part.replace(".", "", 1).isdigit():
                num = float(num_part)
            else:
                return ""
        else:
            num = float(m.group(1))
            unit = m.group(2).upper()

        if unit == "G":
            mbps = int(round(num * 1000))
        elif unit == "M" or unit == "":
            mbps = int(round(num))
        elif unit == "K":
            mbps = int(round(num / 1000.0))
        else:
            return ""
        return str(mbps)

    # ----------------- Transform -----------------
    def _transform_one(
        self,
        rec: Dict,
        mac_map: Dict[str, str],
        addr_map: Dict[Tuple[str, str], str],
        link_map: Dict[Tuple[str, str], Dict],
        port_map: Dict[Tuple[str, str], Dict],
        procdev_map: Dict[Tuple[str, str], Dict],
    ) -> Dict:
        hostname = rec.get("hostname", "") or rec.get("name", "")
        ifname = rec.get("ifname", "") or rec.get("name", "")
        host_l = hostname.lower()
        key = (host_l, ifname)

        link = link_map.get(key, {})
        port = port_map.get(key, {})
        proc = procdev_map.get(key, {})
        ip_addr = addr_map.get(key, "")

        mtu = self._first_nonempty(
            link.get("mtu"),
            port.get("mtu"),
            rec.get("mtu"),
            self._parse_mtu_from_text(rec.get("description") or rec.get("details") or "")
        )

        raw_speed = self._first_nonempty(
            port.get("speed"),
            proc.get("port_speed"),
            rec.get("speed")
        )
        speed_mbps = self._speed_to_mbps_int(raw_speed)

        return {
            "device_mac_address": mac_map.get(host_l, "0.0.0.0"),
            "if_name": ifname,
            "ip_address": ip_addr,
            "oper_status": self._first_nonempty(
                link.get("oper_state"),
                port.get("oper_state"),
                rec.get("oper_state"),
                rec.get("state"),
            ),
            "admin_status": self._first_nonempty(
                link.get("admin_state"),
                port.get("admin_state"),
                rec.get("admin_state"),
            ),
            "alias": self._first_nonempty(
                rec.get("ifalias"),
                port.get("alias"),
            ),
            "autoneg": self._first_nonempty(
                port.get("autoneg"),
                rec.get("autoneg"),
            ),
            "description": self._first_nonempty(
                rec.get("description"),
                rec.get("details"),
            ),
            "mtu": mtu,
            "fec": self._first_nonempty(
                port.get("fec"),
                rec.get("fec"),
            ),
            "breakout_mode": self._first_nonempty(
                port.get("breakout_mode"),
                rec.get("breakout_mode"),
            ),
            "speed": speed_mbps,
            "lanes": self._first_nonempty(
                port.get("lanes"),
                rec.get("lanes"),
            ),
            "index": self._first_nonempty(
                port.get("ifindex"),
                link.get("ifindex"),
                rec.get("ifindex"),
            ),
            "dataconnector_name": self.connector_name,
            "dataconnector_type": self.connector_type,
            "dataconnector_tags": self.connector_tags,
            "deleted": False,
        }

    # ----------------- Publish -----------------
    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("Auth failed (interface inventory)")

        mac_map = self._fetch_inventory_mac_map(token)
        addr_map = self._fetch_address_map(token)
        link_map = self._fetch_links_map(token)
        port_map = self._fetch_port_map(token)
        procdev_map = self._fetch_procdev_map(token)

        rows = self._fetch_interfaces(token)
        log.info("[Interface] fetched %d rows", len(rows))

        sent = 0
        for rec in rows:
            payload = self._transform_one(rec, mac_map, addr_map, link_map, port_map, procdev_map)
            self.producer.send(self.topic, value=payload)
            sent += 1

        self.producer.flush()
        log.info("[Interface] published %d records → topic=%s", sent, self.topic)
