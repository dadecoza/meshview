"""Read-only Meshtastic Monday scoring, adapted from meshinfo-lite.

Score unique local Mondays per node, rather than the number of repeated messages.
Only broadcast text is eligible; private message payloads never enter the query.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from google.protobuf.message import DecodeError
from sqlalchemy import select

from meshtastic.protobuf.mesh_pb2 import MeshPacket
from meshtastic.protobuf.portnums_pb2 import PortNum
from meshview import database
from meshview.models import Node, Packet

logger = logging.getLogger(__name__)
BROADCAST = 0xFFFFFFFF
WEEK = timedelta(days=7)
CACHE_SECONDS = 30


@dataclass(frozen=True)
class CheckIn:
    packet_id: int
    node_id: int
    long_name: str | None
    short_name: str | None
    channel: str | None
    text: str
    timestamp_us: int


class Scoreboard:
    """Aggregate in one pass; sets make weekly deduplication constant time."""

    def __init__(self, timezone: ZoneInfo, now: datetime):
        self.timezone = timezone
        self.now_us = int(now.timestamp() * 1_000_000)
        self.check_ins: dict[tuple[int, date], dict] = {}

    def add(self, check_in: CheckIn) -> None:
        if "meshtasticmonday" not in check_in.text.casefold():
            return
        if not 0 <= check_in.timestamp_us <= self.now_us or check_in.node_id in (0, BROADCAST):
            return
        try:
            local_time = datetime.fromtimestamp(check_in.timestamp_us / 1_000_000, self.timezone)
        except (OverflowError, OSError, ValueError):
            return
        if local_time.weekday() != 0:
            return
        key = (check_in.node_id, local_time.date())
        previous = self.check_ins.get(key)
        # Keep the first check-in, even when the source stream arrives out of order.
        if previous is None or check_in.timestamp_us < previous['timestamp_us']:
            self.check_ins[key] = {
                **vars(check_in),
                'monday': local_time.date(),
                'local_time': local_time.strftime('%Y-%m-%d %H:%M:%S'),
            }

    def finish(self) -> dict:
        days_by_node: dict[int, set[date]] = {}
        nodes = {}
        for (node_id, day), message in self.check_ins.items():
            days_by_node.setdefault(node_id, set()).add(day)
            nodes[node_id] = message
        latest = max((day for _, day in self.check_ins), default=None)
        leaderboard = []
        for node_id, days in days_by_node.items():
            streak = 0
            day = latest
            while day in days:
                streak += 1
                day -= WEEK
            best_streak = 0
            for day in days:
                if day - WEEK in days:
                    continue
                length = 0
                while day in days:
                    length += 1
                    day += WEEK
                best_streak = max(best_streak, length)
            message = nodes[node_id]
            leaderboard.append(
                {
                    'node_id': node_id,
                    'long_name': message['long_name'] or f'!{node_id:08x}',
                    'short_name': message['short_name'] or '',
                    'check_ins': len(days),
                    'streak': streak,
                    'best_streak': best_streak,
                    'last_monday': max(days),
                }
            )
        leaderboard.sort(key=lambda n: (-n['check_ins'], -n['streak'], n['node_id']))
        return {
            'leaderboard': leaderboard,
            'messages': sorted(
                self.check_ins.values(), key=lambda m: (-m['timestamp_us'], m['node_id'])
            ),
            'latest_monday': latest,
            'total_check_ins': len(self.check_ins),
            'participants': len(leaderboard),
            'latest_participants': sum(latest in days for days in days_by_node.values()),
            'timezone': self.timezone.key,
        }


async def load_summary(timezone: ZoneInfo, channel: str = '') -> dict:
    """Stream only needed columns; avoid ORM relationships and scanning other packet types."""
    board = Scoreboard(timezone, datetime.now(UTC))
    query = (
        select(
            Packet.id,
            Packet.from_node_id,
            Packet.channel,
            Packet.payload,
            Packet.import_time_us,
            Node.long_name,
            Node.short_name,
        )
        .outerjoin(Node, Packet.from_node_id == Node.node_id)
        .where(Packet.portnum == PortNum.TEXT_MESSAGE_APP, Packet.to_node_id == BROADCAST)
        .execution_options(yield_per=500)
    )
    if channel:
        query = query.where(Packet.channel == channel)
    async with database.async_session() as session:
        rows = await session.stream(query)
        async for row in rows:
            if row.payload is None or row.import_time_us is None or row.from_node_id is None:
                continue
            try:
                packet = MeshPacket.FromString(row.payload)
                if (
                    packet.decoded.portnum != PortNum.TEXT_MESSAGE_APP
                    or packet.to != BROADCAST
                    or getattr(packet, 'from') != row.from_node_id
                ):
                    continue
                text = packet.decoded.payload.decode('utf-8')
            except (DecodeError, UnicodeDecodeError):
                continue
            board.add(
                CheckIn(
                    row.id,
                    row.from_node_id,
                    row.long_name,
                    row.short_name,
                    row.channel,
                    text,
                    row.import_time_us,
                )
            )
    return board.finish()


_cache = None
_cache_key = None
_expires = 0.0
_lock = asyncio.Lock()


async def get_summary(settings: dict) -> dict:
    """Share a short-lived summary across requests, rebuilding once on cache misses."""
    global _cache, _cache_key, _expires
    timezone_name = settings.get('timezone', 'Africa/Johannesburg')
    try:
        timezone = ZoneInfo(timezone_name)
    except (ZoneInfoNotFoundError, ValueError):
        logger.warning('Invalid Monday timezone %r; using Africa/Johannesburg', timezone_name)
        timezone = ZoneInfo('Africa/Johannesburg')
    channel = settings.get('channel', '').strip()
    key = (timezone.key, channel, id(database.async_session))
    async with _lock:
        if _cache is None or _cache_key != key or time.monotonic() >= _expires:
            _cache = await load_summary(timezone, channel)
            _cache_key = key
            _expires = time.monotonic() + CACHE_SECONDS
        return _cache
