import os
import json
import logging
import requests
from kafka import KafkaProducer
from typing import Optional, List, Dict, Set, Tuple
from datetime import timedelta, datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("collector.inventory.device")


class DeviceCollector:
    def __init__(
        self,
        producer: KafkaProducer,
        connector_name: str,
        connector_type: str,
        connector_tags: str,
        topic: str,
        login_url: str,
        inventory_url: str,
        node_url: str,
        address_url: str,
        username: str,
        password: str,
    ):
        self.producer = producer
        self.topic = topic
        self.connector_name = connector_name
        self.connector_type = connector_type
        self.connector_tags = connector_tags
        self.login_url = login_url
        self.inventory_url = inventory_url
        self.node_url = node_url
        self.address_url = address_url
        self.username = username
        self.password = password
        self.node_hostname_url_template = f"{self.node_url}/hostname/{{hostname}}"
        self.logger = logging.getLogger("collector.inventory.device")
        self._prev_macs: Set[str] = set()

    def _get_token(self) -> Optional[str]:
        try:
            res = requests.post(
                self.login_url,
                json={"username": self.username, "password": self.password},
                verify=False,
            )
            res.raise_for_status()
            return res.json().get("access_token")
        except Exception as e:
            self.logger.error(f"Login failed: {e}")
            return None

    def _get_inventory(self, token: str) -> List[Dict]:
        try:
            headers = {"Authorization": f"Bearer {token}"}
            res = requests.get(
                self.inventory_url + "?label=switches&offset=0&count=200",
                headers=headers,
                verify=False,
            )
            res.raise_for_status()
            body = res.json()
            return body if isinstance(body, list) else body.get("data", [])
        except Exception as e:
            self.logger.warning(f"Inventory fetch failed: {e}")
            return []

    def _get_status_map(self, token: str) -> Dict[str, Tuple[bool, bool]]:
        headers = {"Authorization": f"Bearer {token}"}
        result = {}

        try:
            res = requests.get(self.node_url, headers=headers, verify=False, timeout=10)
            res.raise_for_status()
            rows = res.json()

            for row in rows:
                hostname = row.get("hostname", "").lower()
                is_reachable = bool(row.get("active", False))
                agent_state = (row.get("agent_state") or "").lower()
                status = agent_state in ("fresh", "active")

                if hostname:
                    result[hostname] = (is_reachable, status)

        except Exception as e:
            self.logger.warning(f"Status fetch failed: {e}")

        return result

    def _get_uptime_map(self, token: str, inventory: List[Dict]) -> Dict[str, Tuple[str, int]]:
        result: Dict[str, Tuple[str, int]] = {}
        headers = {"Authorization": f"Bearer {token}"}

        for device in inventory:
            hostname = device.get("hostname", "")
            if not hostname:
                continue

            try:
                res = requests.get(
                    self.node_hostname_url_template.format(hostname=hostname),
                    headers=headers,
                    verify=False,
                    timeout=5,
                )
                if res.status_code != 200:
                    self.logger.warning(f"Failed to fetch uptime for {hostname}: {res.status_code}")
                    continue

                data = res.json()
                if isinstance(data, list) and data:
                    data = data[0]

                ms_uptime = data.get("sys_uptime") or data.get("sysUptime")
                lastboot = data.get("lastboot")

                uptime_seconds = 0
                uptime_str = ""

                if isinstance(lastboot, int) and lastboot > 0:
                    now_ms = int(datetime.utcnow().timestamp() * 1000)
                    raw_uptime_ms = now_ms - lastboot
                    uptime_seconds = abs(raw_uptime_ms // 1000)
                    uptime_str = str(timedelta(seconds=uptime_seconds))
                elif isinstance(ms_uptime, int) and ms_uptime >= 0:
                    uptime_seconds = abs(ms_uptime // 1000)
                    uptime_str = str(timedelta(seconds=uptime_seconds))
                else:
                    self.logger.warning(f"Missing both sys_uptime and lastboot for {hostname}")

                result[hostname.lower()] = (uptime_str, uptime_seconds)

            except Exception as e:
                self.logger.warning(f"Exception while processing uptime for {hostname}: {e}")

        return result

    def _get_ip_map(self, token: str) -> dict:
        result = {}
        headers = {"Authorization": f"Bearer {token}"}

        try:
            res = requests.get(self.address_url, headers=headers, verify=False, timeout=10)
            res.raise_for_status()
            rows = res.json()

            if not isinstance(rows, list):
                self.logger.warning(f"[!] /object/address unexpected payload: {type(rows)}")
                return result

            for row in rows:
                host = row.get("hostname")
                vrf = (row.get("vrf") or "").lower()
                ip = row.get("prefix")

                if host and ip and vrf == "mgmt":
                    host_l = host.lower().split(".", 1)[0]
                    result[host_l] = ip

        except Exception as e:
            self.logger.warning(f"Address fetch failed: {e}")

        return result

    def _enrich(self, rec: Dict) -> Dict:
        rec["dataconnector_name"] = self.connector_name
        rec["dataconnector_type"] = self.connector_type
        rec["dataconnector_tags"] = self.connector_tags
        return rec

    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("Auth failed")

        inventory = self._get_inventory(token)
        uptime_map = self._get_uptime_map(token, inventory)
        ip_map = self._get_ip_map(token)
        status_map = self._get_status_map(token)
        current_macs: Set[str] = set()

        for s in inventory:
            hostname = s.get("hostname", "")
            normalized_hostname = hostname.lower()
            mac = (s.get("platform_base_mac") or "").strip() or hostname

            ip = ip_map.get(normalized_hostname, mac)
            uptime, uptime_seconds = uptime_map.get(normalized_hostname, ("", 0))
            is_reachable, status = status_map.get(normalized_hostname, (True, True))

            payload = {
                "ip_address": ip,
                "hostname": hostname,
                "hwsku": "",
                "os_version": s.get("os_version", ""),
                "uptime": uptime,
                "uptime_seconds": uptime_seconds,
                "serial_number": s.get("platform_serial_number", ""),
                "platform": s.get("platform_vendor", ""),
                "macaddress": mac,
                "model": s.get("platform_model", ""),
                "asic": s.get("asic_model", ""),
                "is_available": True,
                "is_reachable": is_reachable,
                "layer": "",
                "region": "",
                "type": "External",
                "status": status,
                "device_type": s.get("device_type", ""),
                "onie_version": "",
                "kernel_version": s.get("os_version", ""),
                "deleted": False,
            }

            self.producer.send(
                self.topic,
                key=mac.encode(),
                value=self._enrich(payload),
            )
            current_macs.add(mac)

        deleted = self._prev_macs - current_macs
        for mac in deleted:
            self.producer.send(
                self.topic,
                key=mac.encode(),
                value=self._enrich({"macaddress": mac, "deleted": True}),
            )

        self._prev_macs = current_macs
        self.logger.info("Device inventory collection completed successfully.")


