import asyncio
import structlog
from datetime import datetime, timezone
from typing import Literal, Optional, TypedDict

log = structlog.get_logger()


class DecisionResult(TypedDict):
    mode: Literal["CHARGE", "DISCHARGE", "HOLD"]
    target_w: int
    reason: str


class DecisionLoop:
    def __init__(self, settings, influx_client):
        self.settings = settings
        self.influx = influx_client
        self.interval = settings.ems.decision_interval_sec
        self._bucket = settings.influx.bucket
        self._org = settings.influx.org
        self._current_result: DecisionResult = {
            "mode": "HOLD",
            "target_w": 0,
            "reason": "initial state",
        }

    @property
    def current_result(self) -> DecisionResult:
        return self._current_result

    @property
    def current_decision(self) -> str:
        return self._current_result["mode"]

    async def run_forever(self):
        log.info("decision_loop.start", interval=self.interval)
        while True:
            try:
                await self._tick()
            except Exception:
                log.exception("decision_loop.error")
            await asyncio.sleep(self.interval)

    async def _tick(self):
        now = datetime.now(timezone.utc)
        log.info("decision_loop.tick", ts=now.isoformat())
        current_price = await self._fetch_current_price_ct()
        today_prices = await self._fetch_today_forecast_ct()

        if current_price is None:
            result: DecisionResult = {
                "mode": "HOLD",
                "target_w": 0,
                "reason": "missing current price",
            }
        elif not today_prices:
            result = {
                "mode": "HOLD",
                "target_w": 0,
                "reason": "missing today forecast",
            }
        else:
            p25 = self._percentile(today_prices, 0.25)
            p75 = self._percentile(today_prices, 0.75)
            spread = p75 - p25

            if current_price <= p25 and spread >= 8.0:
                result = {
                    "mode": "CHARGE",
                    "target_w": 3000,
                    "reason": f"price={current_price:.2f}<=P25={p25:.2f}, spread={spread:.2f}ct",
                }
            elif current_price >= p75 and spread >= 8.0:
                result = {
                    "mode": "DISCHARGE",
                    "target_w": 3000,
                    "reason": f"price={current_price:.2f}>=P75={p75:.2f}, spread={spread:.2f}ct",
                }
            else:
                result = {
                    "mode": "HOLD",
                    "target_w": 0,
                    "reason": f"price={current_price:.2f}, P25={p25:.2f}, P75={p75:.2f}, spread={spread:.2f}ct",
                }

        self._current_result = result
        log.info(
            "decision_loop.result",
            mode=result["mode"],
            target_w=result["target_w"],
            reason=result["reason"],
        )

    async def _fetch_current_price_ct(self) -> Optional[float]:
        query = f'''
from(bucket: "{self._bucket}")
  |> range(start: -6h)
  |> filter(fn: (r) => r._measurement == "electricity_price")
  |> filter(fn: (r) => r._field == "total")
  |> last()
'''
        tables = await self.influx.query_api().query(query=query, org=self._org)
        for table in tables:
            for record in table.records:
                value = record.get_value()
                if value is not None:
                    return float(value)
        return None

    async def _fetch_today_forecast_ct(self) -> list[float]:
        query = f'''
from(bucket: "{self._bucket}")
  |> range(start: -36h, stop: 36h)
  |> filter(fn: (r) => r._measurement == "electricity_price_forecast")
  |> filter(fn: (r) => r._field == "total")
  |> filter(fn: (r) => r.period == "today")
'''
        tables = await self.influx.query_api().query(query=query, org=self._org)
        prices_by_time: dict[datetime, float] = {}
        for table in tables:
            for record in table.records:
                ts = record.get_time()
                value = record.get_value()
                if ts is None or value is None:
                    continue
                prices_by_time[ts] = float(value)
        return sorted(prices_by_time.values())

    @staticmethod
    def _percentile(values: list[float], q: float) -> float:
        if not values:
            raise ValueError("values cannot be empty")
        if len(values) == 1:
            return values[0]
        idx = (len(values) - 1) * q
        lo = int(idx)
        hi = min(lo + 1, len(values) - 1)
        frac = idx - lo
        return values[lo] + (values[hi] - values[lo]) * frac
