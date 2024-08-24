# Adapted from https://github.com/sabuhish/fastapi-mqtt

from __future__ import annotations

import functools
import json
import operator
import os
from contextlib import asynccontextmanager
from typing import Any

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi_mqtt import FastMQTT, MQTTConfig
from gmqtt import Client as MQTTClient

from ..models import *

# TODO: take .env path
load_dotenv()

ENV = os.environ

mqtt_config = MQTTConfig(
    host=ENV.get("MQTT_HOST", None), port=ENV.get("MQTT_PORT", None)
)
fast_mqtt = FastMQTT(config=mqtt_config)


ts_username = "postgres"
ts_password = "password"
ts_host = "localhost"
ts_port = 5432
ts_database = "postgres"


db_url = f"postgresql://{ENV.get('TIMESCALEDB_USER')}:{ENV.get('TIMESCALEDB_PASS')}@{ENV.get('TIMESCALEDB_HOST')}:{ENV.get('TIMESCALEDB_PORT')}/{ENV.get('TIMESCALEDB_DATABASE')}"

conn = psycopg2.connect(db_url)


def functools_reduce_iconcat(a):
    return functools.reduce(operator.iconcat, a, [])


def get_enviro_sensors():
    query = "SELECT uid from enviro_sensors;"

    with conn:
        with conn.cursor() as curs:
            curs.execute(query)
            sensor_uids = curs.fetchall()

    return functools_reduce_iconcat(sensor_uids)


async def create_db_and_tables():
    EnviroSensor.create_table(conn)
    EnviroGrow.create_table(conn)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    await create_db_and_tables()
    await fast_mqtt.mqtt_startup()
    yield
    await fast_mqtt.mqtt_shutdown()


app = FastAPI(lifespan=_lifespan)


@fast_mqtt.on_connect()
def connect(client: MQTTClient, flags: int, rc: int, properties: Any):
    client.subscribe("/mqtt")  # subscribing mqtt topic
    print("Connected: ", client, flags, rc, properties)


@fast_mqtt.on_message()
async def message(
    client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
):
    print("Received message: ", topic, payload.decode(), qos, properties)


@fast_mqtt.subscribe("/fastscaledb/testdevice")
async def test_device_message(
    client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
):
    test_device = TestDevice.model_validate(json.loads(payload))

    test_device.insert(conn)


@fast_mqtt.subscribe("/fastscaledb/enviro", "enviro/office-plants-1")
async def enviro_message(
    client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
):
    enviro_grow = EnviroGrow.model_validate_json(payload)

    if enviro_grow.uid not in get_enviro_sensors():
        sensor = EnviroSensor.model_validate(enviro_grow, from_attributes=True)

        sensor.insert(conn)

    enviro_grow.insert(conn)


@fast_mqtt.on_disconnect()
def disconnect(client: MQTTClient, packet, exc=None):
    print("Disconnected")


@fast_mqtt.on_subscribe()
def subscribe(client: MQTTClient, mid: int, qos: int, properties: Any):
    print("subscribed", client, mid, qos, properties)


@app.get("/test")
async def func():
    fast_mqtt.publish("/mqtt", "Hello from Fastapi")  # publishing mqtt topic
    return {"result": True, "message": "Published"}
