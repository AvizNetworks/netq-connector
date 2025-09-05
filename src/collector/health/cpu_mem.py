#!/usr/bin/env python3
import requests
import logging
from typing import Dict, List, Optional
from kafka import KafkaProducer

log = logging.getLogger("collector.resource.cpu_memory")


class CPUAndMemoryCollector:
    def __init__(
        self,
        producer: KafkaProducer,
        cpu_topic: str,
        memory_topic: str,
        login_url: str,
        inventory_url: str,
        base_resource_url: str,
        username: str,
        password: str,
        connector_name: str,
        connector_type: str,
        connector_tags: str,
    ):
        self.producer = producer
        self.cpu_topic = cpu_topic
        self.memory_topic = memory_topic
        self.login_url = login_url
        self.inventory_url = inventory_url
        self.base_resource_url = base_resource_url
        self.username = username
        self.password = password
        self.connector_name = connector_name
        self.connector_type = connector_type
        self.connector_tags = connector_tags

    def _get_token(self) -> Optional[str]:
        try:
            res = requests.post(
                self.login_url,
                json={"username": self.username, "password": self.password},
                verify=False,
                timeout=10
            )
            res.raise_for_status()
            return res.json().get("access_token")
        except Exception as e:
            log.error("[CPU/MEM] Auth failed: %s", e)
            return None

    def _fetch_json(self, url: str, token: str) -> List[Dict]:
        headers = {"Authorization": f"Bearer {token}"}
        try:
            res = requests.get(url, headers=headers, verify=False, timeout=10)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            log.warning("[CPU/MEM] Fetch failed (%s): %s", url, e)
            return []

    def _get_mac_map(self, token: str) -> Dict[str, str]:
        rows = self._fetch_json(self.inventory_url, token)
        out = {}
        for row in rows:
            host = (row.get("hostname") or "").lower()
            mac = row.get("platform_base_mac")
            if host and mac:
                out[host] = mac
        return out

    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("Auth failed")

        mac_map = self._get_mac_map(token)
        cpu_records, mem_records = 0, 0

        for hostname, mac in mac_map.items():
            # Fetch CPU data
            cpu_url = f"{self.base_resource_url}/{hostname}/cpu"
            cpu_rows = self._fetch_json(cpu_url, token)
            for row in cpu_rows:
                payload = {
                    "device_mac_address": mac,
                    "time": row.get("timestamp"),
                    "cpu_util": self._safe_float(row.get("cpu_utilization")),
                    "dataconnector_name": self.connector_name,
                    "dataconnector_type": self.connector_type,
                    "dataconnector_tags": self.connector_tags,
                }
                self.producer.send(self.cpu_topic, value=payload)
                cpu_records += 1

            # Fetch Memory data
            mem_url = f"{self.base_resource_url}/{hostname}/memory"
            mem_rows = self._fetch_json(mem_url, token)
            for row in mem_rows:
                payload = {
                    "device_mac_address": mac,
                    "time": row.get("timestamp"),
                    "mem_util": self._safe_float(row.get("mem_utilization")),
                    "dataconnector_name": self.connector_name,
                    "dataconnector_type": self.connector_type,
                    "dataconnector_tags": self.connector_tags,
                }
                self.producer.send(self.memory_topic, value=payload)
                mem_records += 1

        self.producer.flush()
        log.info("[CPU/MEM] Published %d CPU and %d Memory records", cpu_records, mem_records)

    @staticmethod
    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
