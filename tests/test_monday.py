"""Monday scoring boundaries, privacy, deduplication, and the real page route."""

import sys
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from meshtastic.protobuf.mesh_pb2 import MeshPacket
from meshview import database, models, monday
from meshview.monday import CheckIn, Scoreboard

ZONE = ZoneInfo('Africa/Johannesburg')
NOW = datetime(2026, 11, 12, 12, tzinfo=UTC)


def check_in(node=1, day='2026-11-09', *, text='#MeshtasticMonday', stamp=None, packet_id=1):
    stamp = stamp or f'{day}T10:00:00+02:00'
    return CheckIn(
        packet_id,
        node,
        f'Node {node}',
        'N',
        'LongFast',
        text,
        int(datetime.fromisoformat(stamp).timestamp() * 1_000_000),
    )


def test_unique_weeks_and_streak_restart_after_a_gap():
    board = Scoreboard(ZONE, NOW)
    for day in ('2026-10-12', '2026-10-26', '2026-11-02', '2026-11-09'):
        board.add(check_in(day=day))
    board.add(check_in(day='2026-11-09', text='Again #MESHTASTICMONDAY', packet_id=2))
    board.add(check_in(node=2, day='2026-11-02'))
    summary = board.finish()
    first, second = summary['leaderboard']
    assert (first['check_ins'], first['streak'], first['best_streak']) == (4, 3, 3)
    assert (second['check_ins'], second['streak'], second['best_streak']) == (1, 0, 1)
    assert summary['participants'] == 2
    assert summary['total_check_ins'] == 5
    assert summary['latest_participants'] == 1
    assert len(summary['messages']) == 5


def test_out_of_order_retransmits_keep_first_check_in():
    board = Scoreboard(ZONE, NOW)
    board.add(check_in(stamp='2026-11-09T12:00:00+02:00', text='Later #meshtasticmonday'))
    board.add(check_in(stamp='2026-11-09T08:00:00+02:00', text='First #meshtasticmonday'))
    assert board.finish()['messages'][0]['text'] == 'First #meshtasticmonday'


@pytest.mark.parametrize(
    ('stamp', 'qualifies'),
    [
        ('2026-11-08T21:59:59+00:00', False),
        ('2026-11-08T22:00:00+00:00', True),
        ('2026-11-09T21:59:59+00:00', True),
        ('2026-11-09T22:00:00+00:00', False),
        ('2026-11-16T10:00:00+02:00', False),
    ],
)
def test_local_monday_boundaries_and_future_timestamps(stamp, qualifies):
    board = Scoreboard(ZONE, NOW)
    board.add(check_in(stamp=stamp))
    assert board.finish()['total_check_ins'] == int(qualifies)


def test_old_streak_can_be_longer_than_current_streak():
    board = Scoreboard(ZONE, NOW)
    for day in ('2026-10-05', '2026-10-12', '2026-10-19', '2026-11-09'):
        board.add(check_in(day=day))
    node = board.finish()['leaderboard'][0]
    assert (node['streak'], node['best_streak']) == (1, 3)


def test_empty_and_unrelated_messages():
    board = Scoreboard(ZONE, NOW)
    board.add(check_in(text='Hello mesh'))
    board.add(check_in(node=0xFFFFFFFF))
    summary = board.finish()
    assert summary['leaderboard'] == []
    assert summary['latest_monday'] is None
    assert summary['total_check_ins'] == 0


def packet_row(
    packet_id, *, to=0xFFFFFFFF, port=1, payload=None, text='#MeshtasticMonday', channel='LongFast'
):
    p = MeshPacket(to=to)
    setattr(p, 'from', 123)
    p.decoded.portnum = port
    p.decoded.payload = text.encode()
    return models.Packet(
        id=packet_id,
        from_node_id=123,
        to_node_id=to,
        portnum=port,
        payload=p.SerializeToString() if payload is None else payload,
        import_time_us=check_in().timestamp_us,
        channel=channel,
    )


@pytest_asyncio.fixture
async def history(monkeypatch):
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(database, 'async_session', sessions)
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    async with sessions() as session:
        session.add(models.Node(id='!0000007b', node_id=123, long_name='<script>node</script>'))
        session.add_all(
            [
                packet_row(1, text='<script>alert(1)</script> #MeshtasticMonday'),
                packet_row(2, text='#meshtasticmonday duplicate'),
                packet_row(3, to=456, text='PRIVATE #meshtasticmonday'),
                packet_row(4, port=3),
                packet_row(5, payload=b'\xff'),
                packet_row(6, channel='Other'),
                packet_row(7, payload=b''),
            ]
        )
        await session.commit()
    yield sessions
    await engine.dispose()


@pytest.mark.asyncio
async def test_query_excludes_private_and_nontext_and_handles_bad_packets(history):
    summary = await monday.load_summary(ZONE)
    assert summary['total_check_ins'] == 1
    assert summary['participants'] == 1
    assert 'PRIVATE' not in str(summary)
    assert summary['leaderboard'][0]['long_name'] == '<script>node</script>'
    assert (await monday.load_summary(ZONE, 'Missing'))['total_check_ins'] == 0
    assert (await monday.load_summary(ZONE, 'Other'))['total_check_ins'] == 1


@pytest.mark.asyncio
async def test_inconsistent_packet_cannot_publish_private_message(history):
    async with history() as session:
        # Corrupt metadata claims broadcast, while the encoded packet is actually private.
        row = packet_row(8, to=1234, text='SECRET #meshtasticmonday')
        row.to_node_id = 0xFFFFFFFF
        session.add(row)
        await session.commit()
    assert 'SECRET' not in str(await monday.load_summary(ZONE))


@pytest.mark.asyncio
async def test_cache_coalesces_requests_and_rebuilds_for_settings(monkeypatch):
    import asyncio

    calls = []

    async def load(timezone, channel):
        calls.append((timezone.key, channel))
        await asyncio.sleep(0)
        return {'participants': len(calls)}

    monkeypatch.setattr(monday, 'load_summary', load)
    monkeypatch.setattr(monday, '_cache', None)
    monkeypatch.setattr(monday, '_lock', asyncio.Lock())
    first, second = await asyncio.gather(monday.get_summary({}), monday.get_summary({}))
    assert first == second == {'participants': 1}
    assert len(calls) == 1
    await monday.get_summary({'channel': 'LongFast'})
    assert len(calls) == 2
    monkeypatch.setattr(monday, '_expires', 0)
    await monday.get_summary({'channel': 'LongFast'})
    assert len(calls) == 3
    await monday.get_summary({'timezone': 'Does/Not/Exist'})
    assert calls[-1] == ('Africa/Johannesburg', '')


@pytest.mark.asyncio
async def test_monday_route_uses_native_theme_and_escapes_messages(
    history, aiohttp_client, monkeypatch
):
    from aiohttp import web

    # Keep this test runnable in a fresh checkout without a private config.ini.
    sample_config = Path(__file__).resolve().parents[1] / "sample.config.ini"
    monkeypatch.setattr(sys, "argv", ["pytest", "--config", str(sample_config)])

    from meshview import web as web_module

    # Importing web initializes the ordinary read-only engine; point it at the test DB again.
    monkeypatch.setattr(database, 'async_session', history)
    summary = await monday.load_summary(ZONE)

    async def get_summary(settings):
        return summary

    monkeypatch.setattr(monday, 'get_summary', get_summary)
    app = web.Application()
    app.add_routes(web_module.routes)
    client = await aiohttp_client(app)
    for path in ('/monday', '/monday.html'):
        response = await client.get(path)
        assert response.status == 200
        html = await response.text()
        assert 'mesh-data-page mesh-monday-page' in html
        assert 'theme.js' in html
        assert '&lt;script&gt;alert(1)&lt;/script&gt;' in html
        assert '<script>alert(1)</script>' not in html
        assert 'PRIVATE' not in html
    assert (await client.get('/monday?page=invalid')).status == 400
    response = await client.get('/monday?page=999')
    assert response.status == 200
    # Keep leaderboard and log pages independent, and preserve numerical ranks.
    summary['leaderboard'] = [
        {**summary['leaderboard'][0], 'node_id': 1000 + i, 'long_name': f'Participant {i}'}
        for i in range(60)
    ]
    summary['messages'] = [
        {**summary['messages'][0], 'text': f'Check-in {i} #MeshtasticMonday'} for i in range(60)
    ]
    response = await client.get('/monday?page=2&leaderboard_page=2')
    html = await response.text()
    assert response.status == 200
    assert '<td>21</td>' in html
    assert 'Participant 20</a>' in html and 'Participant 0</a>' not in html
    assert 'Check-in 50 #MeshtasticMonday' in html
    assert 'Check-in 0 #MeshtasticMonday' not in html
    assert 'leaderboard_page=2' in html
    assert (await client.get('/monday?leaderboard_page=invalid')).status == 400
