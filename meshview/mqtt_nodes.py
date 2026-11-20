"""Summarize retained MQTT packet observations by gateway and originating node."""

from datetime import UTC, datetime

from sqlalchemy import case, distinct, func, select

from meshview import database
from meshview.models import Node, Packet, PacketSeen

MIN_NODE_ID = 1
MAX_NODE_ID = 0xFFFFFFFE


def _describe_node(row):
    data = dict(row)
    data["last_upload"] = (
        datetime.fromtimestamp(data["last_upload_us"] / 1_000_000, UTC)
        if data["last_upload_us"] is not None
        else None
    )
    return data


async def get_mqtt_nodes(gateway_id: int | None = None) -> list[dict]:
    """Include publishers and senders even when their node metadata is missing."""
    observations = (
        select(
            PacketSeen.node_id.label("gateway_id"),
            Packet.from_node_id.label("source_id"),
            PacketSeen.packet_id,
            PacketSeen.import_time_us,
        )
        .select_from(PacketSeen)
        .join(Packet, Packet.id == PacketSeen.packet_id)
        .where(PacketSeen.node_id.between(MIN_NODE_ID, MAX_NODE_ID))
    )
    if gateway_id is not None:
        observations = observations.where(
            PacketSeen.node_id == gateway_id,
            Packet.from_node_id.between(MIN_NODE_ID, MAX_NODE_ID),
        )
    observations = observations.subquery()
    node_id = observations.c.gateway_id if gateway_id is None else observations.c.source_id
    totals = [
        func.count(distinct(observations.c.packet_id)).label("packets"),
    ]
    if gateway_id is None:
        totals.append(
            func.count(
                distinct(
                    case(
                        (
                            observations.c.source_id.between(MIN_NODE_ID, MAX_NODE_ID),
                            observations.c.source_id,
                        )
                    )
                )
            ).label("sources")
        )
    grouped = (
        select(
            node_id.label("node_id"),
            func.max(observations.c.import_time_us).label("last_upload_us"),
            *totals,
        )
        .group_by(node_id)
        .subquery()
    )
    query = (
        select(grouped, Node.long_name, Node.short_name, Node.hw_model, Node.role)
        .outerjoin(Node, Node.node_id == grouped.c.node_id)
        .order_by(
            grouped.c.last_upload_us.is_(None),
            grouped.c.last_upload_us.desc(),
            grouped.c.node_id,
        )
    )
    async with database.async_session() as session:
        return [_describe_node(row) for row in (await session.execute(query)).mappings()]
