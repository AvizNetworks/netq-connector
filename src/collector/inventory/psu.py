#!/usr/bin/env python3
import logging
import requests
import urllib3
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger("collector.inventory.psu")


class PSUCollector:
    def __init__(
        self,
        producer,
        topic: str,
        login_url: str,
        sensor_url: str,
        inventory_url: str,
        username: str,
        password: str,
        connector_name: str,
        connector_type: str,
        connector_tags: str
    ):
        self.producer = producer
        self.topic = topic
        self.login_url = login_url
        self.sensor_url = sensor_url
        self.inventory_url = inventory_url
        self.username = username
        self.password = password
        self.connector_name = connector_name
        self.connector_type = connector_type
        self.connector_tags = connector_tags

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
            log.error("[PSU] login failed: %s", e)
            return None

    def _fetch_json_list(self, url: str, token: str, what: str) -> List[Dict]:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers, verify=False, timeout=30)
            r.raise_for_status()
            body = r.json()
            return body if isinstance(body, list) else body.get("data", [])
        except Exception as e:
            log.warning("[PSU] fetch %s failed: %s", what, e)
            return []

    def _get_inventory_mac_map(self, token: str) -> Dict[str, str]:
        rows = self._fetch_json_list(self.inventory_url, token, "/object/inventory")
        out: Dict[str, str] = {}
        for row in rows:
            host = (row.get("hostname") or "").lower()
            mac = (row.get("platform_base_mac") or "").strip()
            if host and mac:
                out[host] = mac
        return out

    @staticmethod
    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(val):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None

    def _build_temp_map(self, temp_rows: List[Dict]) -> Dict[Tuple[str, str], List[Dict]]:
        """
        Groups temp sensor rows per PSU (psu1, psu2, etc.) per host
        e.g., ('leaf01', 'psu1') → [sensor1, sensor2]
        """
        out = defaultdict(list)
        for row in temp_rows:
            host = (row.get("hostname") or "").lower()
            s_name = (row.get("s_name") or "").lower()
            if s_name.startswith("psu") and "temp" in s_name:
                psu_prefix = s_name.split("temp")[0]  # "psu1temp2" → "psu1"
                out[(host, psu_prefix)].append(row)
        return out

    def _transform_row(self, row: Dict, mac_map: Dict[str, str], temp_map: Dict[Tuple[str, str], List[Dict]]) -> Dict:
        host_l = (row.get("hostname") or "").lower()
        name = row.get("s_name") or ""
        mac = mac_map.get(host_l, "")

        # Extract temp sensor rows for this PSU unit
        temp_sensors = temp_map.get((host_l, name.lower()), [])
        temp = None
        temp_threshold = None
        if temp_sensors:
            temps = [self._safe_float(r.get("s_temp")) for r in temp_sensors if self._safe_float(r.get("s_temp")) is not None]
            thresholds = [self._safe_float(r.get("s_max")) for r in temp_sensors if self._safe_float(r.get("s_max")) is not None]
            temp = sum(temps) / len(temps) if temps else None
            temp_threshold = max(thresholds) if thresholds else None

        return {
            "device_mac_address": mac,
            "name": name,
            "presence": "",
            "revision": "",
            "serial": "",
            "manufacturer": "",
            "model": "",
            "is_replaceable": "",
            "status": row.get("s_state", ""),
            "led_status": "",
            "temp": temp,
            "temp_threshold": temp_threshold,
            "voltage": self._safe_float(row.get("voltage_input")),
            "voltage_min_threshold": self._safe_float(row.get("s_min")),
            "voltage_max_threshold": self._safe_float(row.get("s_max")),
            "current": None,
            "power": self._safe_float(row.get("power_input")),
            "dataconnector_type": self.connector_type,
            "dataconnector_tags": self.connector_tags
        }

    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("[PSU] auth failed")

        mac_map = self._get_inventory_mac_map(token)
        sensors = self._fetch_json_list(self.sensor_url, token, "/object/sensor")

        psu_rows = [r for r in sensors if (r.get("message_type") or "").lower() == "psu"]
        temp_rows = [r for r in sensors if (r.get("message_type") or "").lower() == "temp"]
        temp_map = self._build_temp_map(temp_rows)

        log.info("[PSU] fetched %d PSU rows, %d temp-mapped PSU units", len(psu_rows), len(temp_map))

        published = 0
        for r in psu_rows:
            payload = self._transform_row(r, mac_map, temp_map)
            self.producer.send(self.topic, value=payload)
            published += 1

        self.producer.flush()
        log.info("[PSU] published %d PSU records → topic=%s", published, self.topic)
