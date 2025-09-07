#!/usr/bin/env python3
import os
import json
import time
import logging
from kafka import KafkaProducer

from collector.inventory.device import DeviceCollector
from collector.inventory.interface import InterfaceCollector
from collector.inventory.fan import FanCollector
from collector.inventory.psu import PSUCollector
from collector.health.cpu_mem import CPUAndMemoryCollector
from collector.health.interface_counters import InterfaceCountersCollector

# ----------------------
# Setup Logging
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("NetQCollector")

def main():
    log.info("Starting NetQ Collector...")

    # --------------------------
    # Load Environment Variables
    # --------------------------
    kafka_bootstrap = os.getenv("KAFKA_BROKER")

    device_topic    = os.getenv("DEVICE_TOPIC")
    iface_topic     = os.getenv("INTERFACE_TOPIC")
    fan_topic       = os.getenv("FAN_TOPIC") 
    psu_topic       = os.getenv("PSU_TOPIC")
    cpu_topic       = os.getenv("CPU_TOPIC")
    memory_topic    = os.getenv("MEMORY_TOPIC")
    iface_counters_topic = os.getenv("INTERFACE_COUNTERS_TOPIC")

    netq_host       = os.getenv("NETQ_URL")
    login_url       = f"{netq_host}/api/netq/auth/v1/login"
    inventory_url   = f"{netq_host}/api/netq/telemetry/v1/object/inventory"
    interface_url   = f"{netq_host}/api/netq/telemetry/v1/object/interface"
    sensor_url      = f"{netq_host}/api/netq/telemetry/v1/object/sensor" 
    base_resource_url = f"{netq_host}/api/netq/telemetry/v1/object/resource/hostname"
    procdevstats_url  = f"{netq_host}/api/netq/telemetry/v1/object/procdevstats"
    node_url        = f"{netq_host}/api/netq/telemetry/v1/object/node"
    address_url     = f"{netq_host}/api/netq/telemetry/v1/object/address"

    username        = os.getenv("NETQ_USERNAME")
    password        = os.getenv("NETQ_PASSWORD")
    connector_name  = os.getenv("CONNECTOR_NAME")
    connector_type  = os.getenv("CONNECTOR_TYPE")
    connector_tags  = os.getenv("CONNECTOR_TAGS", "")
    interval        = int(os.getenv("COLLECTOR_INTERVAL", "30"))

    # Basic env validation
    missing = [k for k,v in {
        "DEVICE_TOPIC":device_topic,
        "INTERFACE_TOPIC":iface_topic,
        "FAN_TOPIC":fan_topic,
        "PSU_TOPIC":psu_topic,
        "CPU_TOPIC":cpu_topic,
        "MEMORY_TOPIC":memory_topic,
        "INTERFACE_COUNTERS_TOPIC":iface_counters_topic,
        "NETQ_HOST":netq_host,
        "NETQ_USERNAME":username,
        "NETQ_PASSWORD":password,
    }.items() if not v]
    if missing:
        log.error(f"Missing required env vars: {', '.join(missing)}")
        return

    # --------------------------
    # Kafka Producer Setup
    # --------------------------
    log.info(f"Connecting to Kafka at: {kafka_bootstrap}")
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # --------------------------
    # Initialize Collectors
    # --------------------------
    device_collector = DeviceCollector(
        producer=producer,
        topic=device_topic,
        login_url=login_url,
        inventory_url=inventory_url,
        node_url=node_url,
        address_url=address_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    iface_collector = InterfaceCollector(
        producer=producer,
        topic=iface_topic,
        login_url=login_url,
        inventory_url=inventory_url,
        interface_url=interface_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    fan_collector = FanCollector(  
        producer=producer,
        topic=fan_topic,
        login_url=login_url,
        inventory_url=inventory_url,
        sensor_url=sensor_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    psu_collector = PSUCollector(
        producer=producer,
        topic=psu_topic,
        login_url=login_url,
        sensor_url=sensor_url,
        inventory_url=inventory_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    cpu_mem_collector = CPUAndMemoryCollector(
        producer=producer,
        cpu_topic=cpu_topic,
        memory_topic=memory_topic,
        login_url=login_url,
        inventory_url=inventory_url,
        base_resource_url=base_resource_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    iface_counters_collector = InterfaceCountersCollector(
        producer=producer,
        topic=iface_counters_topic,
        login_url=login_url,
        procdevstats_url=procdevstats_url,
        inventory_url=inventory_url,
        username=username,
        password=password,
        connector_name=connector_name,
        connector_type=connector_type,
        connector_tags=connector_tags
    )

    # --------------------------
    # Main Collection Loop
    # --------------------------
    while True:
        try:
            log.info("Collecting Device Inventory...")
            device_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"Device inventory failed: {e}", exc_info=True)

        try:
            log.info("Collecting Interface Inventory...")
            iface_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"Interface inventory failed: {e}", exc_info=True)

        try:
            log.info("Collecting Fan Inventory...")
            fan_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"Fan inventory failed: {e}", exc_info=True)

        try:
            log.info("Collecting PSU Inventory...")
            psu_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"PSU inventory failed: {e}", exc_info=True)

        try:
            log.info("Collecting CPU and Memory Utilization...")
            cpu_mem_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"CPU/Memory collection failed: {e}", exc_info=True)

        try:
            log.info("Collecting Interface Counters...")
            iface_counters_collector.collect_and_publish()
        except Exception as e:
            log.warning(f"Interface counters collection failed: {e}", exc_info=True)

        log.info(f"Sleeping for {interval} seconds...\n")
        time.sleep(interval)

if __name__ == "__main__":
    main()


