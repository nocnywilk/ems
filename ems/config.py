import os

from pydantic_settings import BaseSettings
from pydantic import Field

class InfluxConfig(BaseSettings):
    model_config = {"env_prefix": "INFLUX_"}
    url: str = "http://influxdb:8086"
    token: str
    org: str = "ems"
    bucket: str = "energy"

class TibberConfig(BaseSettings):
    model_config = {"env_prefix": "TIBBER_"}
    token: str

class SolcastConfig(BaseSettings):
    model_config = {"env_prefix": "SOLCAST_"}
    api_key: str
    site_1: str
    site_2: str

class SonnenConfig(BaseSettings):
    model_config = {"env_prefix": "SONNEN_"}
    ip: str
    token: str

class EMSConfig(BaseSettings):
    model_config = {"env_prefix": "EMS_"}
    log_level: str = "INFO"
    decision_interval_sec: int = 300
    confirm_interval_sec: int = 60
    dry_run: bool = True

class Settings(BaseSettings):
    influx: InfluxConfig = Field(default_factory=InfluxConfig)
    tibber: TibberConfig = Field(default_factory=TibberConfig)
    solcast: SolcastConfig = Field(default_factory=SolcastConfig)
    sonnen: SonnenConfig = Field(default_factory=SonnenConfig)
    ems: EMSConfig = Field(default_factory=EMSConfig)

def load_settings() -> Settings:
    env_aliases = {
        "INFLUXDB_URL": "INFLUX_URL",
        "INFLUXDB_TOKEN": "INFLUX_TOKEN",
        "INFLUXDB_ORG": "INFLUX_ORG",
        "INFLUXDB_BUCKET": "INFLUX_BUCKET",
        "SONNEN_USER": "SONNEN_IP",
        "SONNEN_PASS": "SONNEN_TOKEN",
    }
    for src, dst in env_aliases.items():
        if src in os.environ and dst not in os.environ:
            os.environ[dst] = os.environ[src]

    defaults = {
        "SOLCAST_SITE_1": "dummy_site_1",
        "SOLCAST_SITE_2": "dummy_site_2",
        "SONNEN_IP": "127.0.0.1",
        "SONNEN_TOKEN": "dummy_sonnen_token",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)

    return Settings()
