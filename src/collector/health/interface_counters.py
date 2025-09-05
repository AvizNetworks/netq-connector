#!/usr/bin/env python3
import requests
import logging
from typing import Dict, List, Optional
from kafka import KafkaProducer

log = logging.getLogger("collector.interface_counters")

class InterfaceCountersCollector:
    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
        login_url: str,
        procdevstats_url: str,
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
        self.procdevstats_url = procdevstats_url
        self.inventory_url = inventory_url
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
            log.error("[InterfaceCounters] Auth failed: %s", e)
            return None

    def _fetch_json(self, url: str, token: str) -> List[Dict]:
        try:
            headers = {"Authorization": f"Bearer {token}"}
            res = requests.get(url, headers=headers, verify=False, timeout=10)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            log.warning("[InterfaceCounters] Fetch failed (%s): %s", url, e)
            return []

    def _get_mac_map(self, token: str) -> Dict[str, str]:
        rows = self._fetch_json(self.inventory_url, token)
        out = {}
        for row in rows:
            hostname = (row.get("hostname") or "").lower()
            mac = row.get("platform_base_mac")
            if hostname and mac:
                out[hostname] = mac
        return out

    def collect_and_publish(self):
        token = self._get_token()
        if not token:
            raise RuntimeError("Auth failed")

        mac_map = self._get_mac_map(token)
        data = self._fetch_json(self.procdevstats_url, token)
        records = 0

        for row in data:
            hostname = (row.get("hostname") or "").lower()
            mac = mac_map.get(hostname, "")
            payload = {
                "device_mac_address": mac,
                "if_name": row.get("ifname", ""),
                "in_octets": self._safe_int(row.get("rx_bytes")),
                "out_octets": self._safe_int(row.get("tx_bytes")),
                "in_errors": self._safe_int(row.get("rx_errs")),
                "out_errors": self._safe_int(row.get("tx_errs")),
                "in_discards": self._safe_int(row.get("rx_drop")),
                "out_discards": self._safe_int(row.get("tx_drop")),
                "in_packets": self._safe_int(row.get("rx_packets")),
                "out_packets": self._safe_int(row.get("tx_packets")),
                "dataconnector_name": self.connector_name,
                "dataconnector_type": self.connector_type,
                "dataconnector_tags": self.connector_tags,
            }
            self.producer.send(self.topic, value=payload)
            records += 1

        self.producer.flush()
        log.info("[InterfaceCounters] Published %d interface counters records", records)

    @staticmethod
    def _safe_int(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0
