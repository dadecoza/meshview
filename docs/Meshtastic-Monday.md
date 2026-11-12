# Meshtastic Monday

This local extension ports meshinfo-lite's weekly check-in idea to Meshview's
existing packet history. The page is `/monday` (also `/monday.html`) and uses
Meshview's shared navigation, tables, typography, responsive layout and day/night
theme. It sends no messages and does not change MQTT ingestion or the database
schema.

## Scoring

- An eligible message is broadcast text containing `meshtasticmonday`, ignoring
  case. Both `#MeshtasticMonday` and the bare keyword qualify, as in meshinfo-lite.
- Monday is determined from the packet's recorded import timestamp in the
  configured timezone. Future timestamps and malformed packets are excluded.
- A node gets one check-in per local Monday across all eligible channels,
  regardless of retransmissions or how many messages it sends. The first message
  for that node and date is shown in the log.
- Rank is total weekly check-ins, then current streak, then node ID for stable ties.
- The current streak counts consecutive Mondays ending on the latest Monday with
  a qualifying check-in anywhere in the selected history. This retains the
  original feature's network-wide anchor, rather than using the viewer's clock.
  A missing latest Monday gives a current streak of zero. A new streak after a
  gap starts at one; an older gap cannot reset a later consecutive run.
- Best streak is the longest consecutive run within the retained history.
- Private messages never enter the SQL query. Encoded packet destinations and
  senders are also checked before scoring. Names and messages are HTML-escaped.

Scores are calculated from retained packet history, not stored in another table.
Enabling retention cleanup can therefore reduce historical totals and streaks.
Imported legacy records qualify using their preserved timestamps; unknown channel
names remain visible as their imported labels.

## Configuration

The page works without changing an existing production config. Defaults are
South African time, all broadcast channels, and a visible navigation link.

```ini
[site]
monday = True

[meshtastic_monday]
timezone = Africa/Johannesburg
channel =
```

Set `channel = LongFast` to restrict scoring to that exact stored channel name.
Leaving it empty also includes imported `legacy-channel-N` history, whose original
channel names are unavailable. Set `site.monday = False` to hide the navigation
link, following the other Meshview navigation switches; this is not access control.
Invalid timezone names are logged and fall back to `Africa/Johannesburg`.

## Implementation and performance

The new `meshview/monday.py` module streams only public text packets and the node
columns needed for display. Weekly deduplication uses dictionaries/sets; streak
calculation visits each retained Monday a bounded number of times. No per-node
SQL queries, ORM relationship loading, or scans of telemetry/traceroute payloads
are needed. A 30-second cache with an async lock shares one rebuild across
concurrent requests. The log is paginated at 50 rows and the leaderboard at 20.

The original implementation repeatedly searched date lists, only removed adjacent
identical messages, and could reset a valid recent streak while processing an
older gap. Tests cover the corrected behavior.

On the local post-migration database copy, the initial implementation found 119
participants and 893 unique weekly check-ins. A cold calculation took approximately
370 ms locally; a cached lookup took under 1 ms. These are local measurements, not
production latency guarantees. The database copy is ignored by Git.

## Local review

Repository: `/home/johannes/src/meshview`

Branch: `feature/meshtastic-monday`

Upstream: `https://github.com/pablorevilla-meshtastic/meshview.git`

Base: `0453de0dd19ce69da144c5e40fa8585b03c1ce00`, matching the Oracle deployment.

This is a local fork/feature branch. No GitHub repository or pull request was
created and no changes were pushed.

A local configuration and SQLite copy are already prepared. Run only the web
server for review; there is no need to connect a local MQTT writer:

```sh
cd /home/johannes/src/meshview
.venv/bin/python main.py
```

Open `http://localhost:8083/monday`. If the preview is already running, use that
instance instead. It binds only to loopback and reads the local database copy.
Screenshots are in the ignored `preview/` directory.

Validation:

```sh
.venv/bin/python -m pytest -q
.venv/bin/ruff check meshview/monday.py meshview/web.py meshview/web_api/api.py tests/test_monday.py
```

The full Python suite passes (30 tests). Browser checks cover desktop and mobile,
day/night themes, navigation, aliases and independent pagination, with no page
JavaScript errors. New labels are included in English, Spanish and Russian.
One existing SQLAlchemy read-only URL warning remains in the upstream database
initialization code; it is outside this feature's changes.

## Production review

The Oracle server has not been changed by this feature work. After local review,
take a current SQLite backup and deploy the reviewed branch using the existing
application deployment process. Only the web service needs to load these changes;
there are no schema migrations, new runtime dependencies or ingestion changes.
Review the scoring timezone/channel policy and history retention before rollout.
