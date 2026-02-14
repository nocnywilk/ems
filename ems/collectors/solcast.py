import httpx
import structlog
from datetime import datetime, timezone, timedelta
from influxdb_client.client.write.point import Point
from .base import BaseCollector

log = structlog.get_logger()

SOLCAST_BASE_URL = "https://api.solcast.com.au"


class SolcastCollector(BaseCollector):
    """
    Collects PV production forecast from Solcast API.

    CRITICAL: Free tier = 10 API calls/day total (5 per site for 2 sites).
    Strategy: fetch every 4 hours = 6 calls/site/day, but we use 5 to stay safe.
      → fetch at: 05:00, 09:00, 13:00, 17:00, 21:00 UTC (5 fetches × 2 sites = 10)

    Sites: SE (South-East roof) and SW (South-West roof)

    Writes to 'energy' bucket:
        measurement: pv_forecast
        fields: pv_estimate (W), pv_estimate10 (W), pv_estimate90 (W)
        tags: source=solcast, site=(se|sw)
        time: period_end from Solcast
    """

    name = "solcast"
    interval_sec = 900  # Check every 15 min, but only fetch at scheduled hours

    # Hours (UTC) at which we actually call the API
    FETCH_HOURS_UTC = {5, 9, 13, 17, 21}

    def __init__(self, settings, influx_client):
        super().__init__(settings, influx_client)
        self._api_key = settings.solcast.api_key
        self._sites = {
            "se": settings.solcast.site_1,
            "sw": settings.solcast.site_2,
        }
        self._client = httpx.AsyncClient(timeout=30.0)
        self._last_fetch_hour: int | None = None
        self._daily_calls = 0
        self._daily_calls_date: str = ""

    def _reset_daily_counter_if_needed(self):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self._daily_calls_date:
            self._daily_calls = 0
            self._daily_calls_date = today
            log.info("solcast.daily_counter_reset", date=today)

    def _should_fetch(self) -> bool:
        """Only fetch at scheduled hours, max once per hour window."""
        now = datetime.now(timezone.utc)
        current_hour = now.hour

        if current_hour not in self.FETCH_HOURS_UTC:
            return False

        if self._last_fetch_hour == current_hour:
            return False

        self._reset_daily_counter_if_needed()
        if self._daily_calls >= 10:
            log.warning("solcast.daily_limit_reached", calls=self._daily_calls)
            return False

        return True

    async def _collect(self):
        if not self._should_fetch():
            log.debug("solcast.skip", reason="not_scheduled_hour")
            return

        now = datetime.now(timezone.utc)
        points = []

        for site_label, site_id in self._sites.items():
            try:
                site_points = await self._fetch_site(site_id, site_label)
                points.extend(site_points)
                self._daily_calls += 1
                log.info(
                    "solcast.site_fetched",
                    site=site_label,
                    site_id=site_id,
                    forecasts=len(site_points),
                    daily_calls=self._daily_calls,
                )
            except Exception:
                log.exception("solcast.site_error", site=site_label)
                # Don't re-raise — try the other site
                continue

        if points:
            await self._write_points(points, bucket="energy")
            self._last_fetch_hour = now.hour

    async def _fetch_site(self, site_id: str, site_label: str) -> list[Point]:
        """Fetch forecast for a single rooftop site."""
        resp = await self._client.get(
            f"{SOLCAST_BASE_URL}/rooftop_sites/{site_id}/forecasts",
            params={"format": "json", "hours": 48},
            headers={"Authorization": f"Bearer {self._api_key}"},
        )
        resp.raise_for_status()
        data = resp.json()

        points = []
        for fc in data.get("forecasts", []):
            period_end = datetime.fromisoformat(fc["period_end"].replace("Z", "+00:00"))

            # Solcast returns kW, convert to W
            pv_estimate = fc.get("pv_estimate", 0) * 1000
            pv_estimate10 = fc.get("pv_estimate10", 0) * 1000
            pv_estimate90 = fc.get("pv_estimate90", 0) * 1000

            points.append(
                Point("pv_forecast")
                .tag("source", "solcast")
                .tag("site", site_label)
                .field("pv_estimate_w", float(pv_estimate))
                .field("pv_estimate10_w", float(pv_estimate10))
                .field("pv_estimate90_w", float(pv_estimate90))
                .time(period_end)
            )

        return points
