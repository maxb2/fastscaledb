from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import psycopg2

from datetime import datetime

from pydantic import BaseModel, Field, model_validator


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
        --- SELECT create_hypertable('sensor_data', by_range('time'));
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


class EnviroSensor(BaseModel):
    uid: str
    nickname: str
    model: str
    location: str | None = Field(default=None)

    @staticmethod
    def create_table(conn: psycopg2.connection):
        query = """
        CREATE TABLE IF NOT EXISTS enviro_sensors (
            uid VARCHAR(16) PRIMARY KEY,
            nickname TEXT NOT NULL,
            model TEXT NOT NULL,
            location TEXT
        );
        """

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def insert(self, conn: psycopg2.connection):

        query = (
            f"INSERT into enviro_sensors (uid, nickname, location) VALUES (%s, %s, %s)"
        )

        cursor = conn.cursor()
        cursor.execute(query, (self.uid, self.nickname, self.location))
        conn.commit()
        cursor.close()


class EnviroGrow(BaseModel):
    nickname: str
    uid: str
    model: str = Field(default="grow")
    timestamp: datetime
    temperature: float
    humidity: float
    pressure: float
    luminance: float
    moisture_a: float
    moisture_b: float
    moisture_c: float
    voltage: float | None = Field(default=None)

    @model_validator(mode="before")
    @classmethod
    def flatten_readings(cls, data: Any) -> Any:
        if isinstance(data, dict):
            readings = data.get("readings")
            if readings is None:
                return data
            return readings | dict(data)
        return data

    @staticmethod
    def create_table(conn: psycopg2.connection):
        query = """
        CREATE TABLE IF NOT EXISTS enviro_grow (
            nickname TEXT NOT NULL,
            uid TEXT NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            luminance DOUBLE PRECISION,
            moisture_a DOUBLE PRECISION,
            moisture_b DOUBLE PRECISION,
            moisture_c DOUBLE PRECISION,
            voltage DOUBLE PRECISION,
            FOREIGN KEY (uid) REFERENCES enviro_sensors (uid)
        );
        --- SELECT create_hypertable('enviro_grow', by_range('timestamp'));
        """

        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def insert(self, conn: psycopg2.connection):
        query = f"""INSERT into enviro_grow (
            nickname,
            uid,
            timestamp,
            temperature,
            humidity,
            pressure,
            luminance,
            moisture_a,
            moisture_b,
            moisture_c,
            voltage
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )"""

        data = (
            self.nickname,
            self.uid,
            self.timestamp,
            self.temperature,
            self.humidity,
            self.pressure,
            self.luminance,
            self.moisture_a,
            self.moisture_b,
            self.moisture_c,
            self.voltage,
        )

        cursor = conn.cursor()
        cursor.execute(query, data)
        conn.commit()
        cursor.close()
