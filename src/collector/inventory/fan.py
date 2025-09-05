#!/usr/bin/env python3
import os
import json
import logging
from typing import Dict, List, Optional

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger("collector.inventory.fan")


class FanCollector:
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
        connector_tags: str,
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
            log.error("[Fan] login failed: %s", e)
            return None

    def _fetch_json_list(self, url: str, token: str, what: str) -> List[Dict]:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            r = requests.get(url, headers=headers, verify=False, timeout=30)
            r.raise_for_status()
            body = r.json()
            return body if isinstance(body, list) else body.get("data", [])
        except Exception as e:
            log.warning("[Fan] fetch %s failed: %s", what, e)
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
    def _to_int_or_blank(v):
        if v is None:
            return ""
        try:
            return int(float(v))
        except Exception:
            return ""

    def _transform_row(self, row: Dict, mac_map: Dict[str, str]) -> Dict:

        host_l = (row.get("hostname") or "").lower()
        mac = mac_map.get(host_l, "")

        name = row.get("s_name") or ""


        status = row.get("s_state", "")


        speed            = self._to_int_or_blank(row.get("s_input"))  
        speed_tolerance  = self._to_int_or_blank(row.get("s_min"))   
        speed_target     = self._to_int_or_blank(row.get("s_max"))   

        return {
            "device_mac_address": mac,
            "name": name,
            "presence": "",
            "model": "",
            "serial": "",
            "drawer_name": "",
            "direction": "",
            "led_status": "",
            "status": status,
            "is_replaceable": "",
            "speed": speed,
            "speed_tolerance": speed_tolerance,
            "speed_target": speed_target,
            "speed_rpm": None,
            "speed_tolerance_rpm": None,
            "speed_target_rpm": None,
            "dataconnector_name": self.connector_name,
            "dataconnector_type": self.connector_type,
            "dataconnector_tags": self.connector_tags
        }


    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("[Fan] auth failed")

        mac_map = self._get_inventory_mac_map(token)
        sensors = self._fetch_json_list(self.sensor_url, token, "/object/sensor")


        fans = [r for r in sensors if (r.get("message_type") or "").lower() == "fan"]
        log.info("[Fan] fetched %d fan sensor rows", len(fans))

        published = 0
        for r in fans:
            payload = self._transform_row(r, mac_map)
            self.producer.send(self.topic, value=payload)
            published += 1

        self.producer.flush()
        log.info("[Fan] published %d fan records → topic=%s", published, self.topic)

