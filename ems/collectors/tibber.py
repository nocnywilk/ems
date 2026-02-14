import httpx
import structlog
from datetime import datetime, timezone
from influxdb_client.client.write.point import Point
from .base import BaseCollector

log = structlog.get_logger()

TIBBER_API_URL = "https://api.tibber.com/v1-beta/gql"

# GraphQL query for price info (current + today + tomorrow)
PRICE_QUERY = """
{
  viewer {
    homes {
      currentSubscription {
        priceInfo {
          current {
            total
            energy
            tax
            startsAt
            level
          }
          today {
            total
            energy
            tax
            startsAt
            level
          }
          tomorrow {
            total
            energy
            tax
            startsAt
            level
          }
        }
      }
    }
  }
}
"""


class TibberCollector(BaseCollector):
    """
    Collects electricity prices from Tibber GraphQL API.

    Interval: 5 min (prices change hourly, but we want responsiveness)

    Writes to 'energy' bucket:
        measurement: electricity_price
        fields: total, energy, tax
        tags: source=tibber, level=(VERY_CHEAP|CHEAP|NORMAL|EXPENSIVE|VERY_EXPENSIVE)

        measurement: electricity_price_forecast
        fields: total, energy, tax
        tags: source=tibber, period=(today|tomorrow), level=...
        time: startsAt of each hourly price
    """

    name = "tibber"
    interval_sec = 300  # 5 min

    def __init__(self, settings, influx_client):
        super().__init__(settings, influx_client)
        self._token = settings.tibber.token
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        self._client = httpx.AsyncClient(timeout=15.0)
        self._last_forecast_hash = None

    async def _collect(self):
        resp = await self._client.post(
            TIBBER_API_URL,
            headers=self._headers,
            json={"query": PRICE_QUERY},
        )
        resp.raise_for_status()
        result = resp.json()

        if "errors" in result:
            raise RuntimeError(f"Tibber GraphQL errors: {result['errors']}")

        homes = result["data"]["viewer"]["homes"]
        if not homes:
            raise RuntimeError("No homes found in Tibber account")

        price_info = homes[0]["currentSubscription"]["priceInfo"]
        points = []

        # 1. Current price → write with current timestamp
        current = price_info["current"]
        if current:
            now = datetime.now(timezone.utc)
            points.append(
                Point("electricity_price")
                .tag("source", "tibber")
                .tag("level", current.get("level", "NORMAL"))
                .field("total", float(current["total"]))
                .field("energy", float(current["energy"]))
                .field("tax", float(current["tax"]))
                .time(now)
            )
            log.info(
                "tibber.current_price",
                total=current["total"],
                level=current.get("level"),
            )

        # 2. Forecast prices (today + tomorrow) → write with startsAt timestamp
        #    Only rewrite if forecast data changed (avoid duplicate writes)
        forecast_points = []
        for period_name, prices in [("today", price_info.get("today", [])),
                                      ("tomorrow", price_info.get("tomorrow", []))]:
            if not prices:
                continue
            for p in prices:
                ts = datetime.fromisoformat(p["startsAt"])
                forecast_points.append(
                    Point("electricity_price_forecast")
                    .tag("source", "tibber")
                    .tag("period", period_name)
                    .tag("level", p.get("level", "NORMAL"))
                    .field("total", float(p["total"]))
                    .field("energy", float(p["energy"]))
                    .field("tax", float(p["tax"]))
                    .time(ts)
                )

        # Simple dedup: hash of forecast totals
        forecast_hash = hash(
            tuple(
                (p["total"], p["startsAt"])
                for period in ["today", "tomorrow"]
                for p in (price_info.get(period) or [])
            )
        )
        if forecast_hash != self._last_forecast_hash:
            points.extend(forecast_points)
            self._last_forecast_hash = forecast_hash
            log.info(
                "tibber.forecast_updated",
                today_hours=len(price_info.get("today") or []),
                tomorrow_hours=len(price_info.get("tomorrow") or []),
            )

        await self._write_points(points, bucket="energy")
