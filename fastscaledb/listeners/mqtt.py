# Adapted from https://github.com/sabuhish/fastapi-mqtt

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from gmqtt import Client as MQTTClient

from fastapi_mqtt import FastMQTT, MQTTConfig

from datetime import datetime

from sqlalchemy import text

from pydantic import BaseModel, Field

import os

import json

from dotenv import load_dotenv

import psycopg2

# TODO: take .env path
load_dotenv()

ENV = os.environ

mqtt_config = MQTTConfig(host=ENV.get("MQTT_HOST", None), port=ENV.get("MQTT_PORT", None))
fast_mqtt = FastMQTT(config=mqtt_config)

class Sensor(BaseModel):
    __tablename__: str = "sensor"

    id: int | None = Field(default=None, primary_key=True)
    name: str
    location: str

    @staticmethod
    def create_table(conn: psycopg2.connection):
        query = """
        CREATE TABLE IF NOT EXISTS sensors (
            id SERIAL PRIMARY KEY,
            name TEXT,
            location TEXT  
        );
        """

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def insert(self, conn: psycopg2.connection):
        query = f"INSERT into sensors (name, location) VALUES ('{self.name}', '{self.location}')"

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()


class TestDevice(BaseModel):
    __tablename__: str = "testdevice"

    time: datetime
    sensor_id: int | None = Field(default=None, foreign_key="sensor.id")
    temperature: float

    @staticmethod
    def create_table(conn: psycopg2.connection):
        query = """
        CREATE TABLE IF NOT EXISTS sensor_data (
            time TIMESTAMPTZ NOT NULL,
            sensor_id INTEGER,
            temperature DOUBLE PRECISION,
            FOREIGN KEY (sensor_id) REFERENCES sensors (id)
        );
        SELECT create_hypertable('sensor_data', by_range('time'));
        """

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def insert(self, conn: psycopg2.connection):
        query = f"INSERT into sensor_data (time, sensor_id, temperature) VALUES ('{self.time.isoformat()}', {self.sensor_id}, {self.temperature})"

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

ts_username = "postgres"
ts_password = "password"
ts_host = "localhost"
ts_port = 5432
ts_database = "postgres"


db_url = f"postgresql://{ts_username}:{ts_password}@{ts_host}:{ts_port}/{ts_database}"

sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

conn = psycopg2.connect(db_url)

# engine = create_engine(db_url, echo=True)

async def create_db_and_tables():
    Sensor.create_table(conn)
    Sensor(name="demo1", location="kitchen").insert(conn)
    Sensor(name="demo2", location="bathroom").insert(conn)
    Sensor(name="demo3", location="bedroom").insert(conn)
    TestDevice.create_table(conn)

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

@fast_mqtt.subscribe("mqtt/+/temperature", "mqtt/+/humidity", qos=1)
async def home_message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    print("temperature/humidity: ", topic, payload.decode(), qos, properties)

@fast_mqtt.on_message()
async def message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    print("Received message: ", topic, payload.decode(), qos, properties)

@fast_mqtt.subscribe("my/mqtt/topic/#", qos=2)
async def message_to_topic_with_high_qos(
    client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any
):
    print(
        "Received message to specific topic and QoS=2: ", topic, payload.decode(), qos, properties
    )

@fast_mqtt.subscribe("/fastscaledb/testdevice")
async def test_device_message(client: MQTTClient, topic: str, payload: bytes, qos: int, properties: Any):
    test_device = TestDevice.model_validate(json.loads(payload))

    test_device.insert(conn)

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
