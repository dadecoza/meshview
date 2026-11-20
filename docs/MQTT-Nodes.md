# MQTT Nodes on the ZR1RF MeshView deployment

The page at `/mqtt-nodes` uses retained `packet_seen` observations. Each
observation identifies the publishing gateway through the Meshtastic service
envelope. The corresponding `packet.from_node_id` identifies the originating
radio. A gateway's detail page includes its own packets when it uploads them.
Packet counts deduplicate the packet ID separately for each gateway. A packet
seen by several gateways can appear under each of them. The last-upload time is
the MeshView ingestion time and is shown in UTC.

Gateways are listed from packet activity, including gateways without NodeInfo and
gateways forwarding through MQTT bridges. Broker session presence and idle
clients are outside what retained packet data can show. The page displays node
metadata and aggregate counts and does not query broker credentials or addresses.
Set `mqtt_nodes = False` under `[site]` to hide the navigation entry.

## Upgrade and rollback

The feature is a standalone Git commit above the deployed Meshtastic Monday
change. It reads the existing Packet, PacketSeen and Node models and adds no
schema, broker, Apache, or ingest configuration changes. Preserve the deployed
branch and its commits in a protected repository or bundle before upgrading.

1. From `/opt/meshview`, record the deployed commit with `git rev-parse HEAD`,
   fetch the target upstream revision, and create a separate worktree and upgrade
   branch at that revision.
2. Carry forward the site's local commits, including Meshtastic Monday and the
   commit titled `Add MQTT gateway and uploaded node views`. Resolve any
   overlaps in `web.py`, `templates/base.html` and `web_api/api.py`. Check
   that `PacketSeen.node_id` still identifies the envelope gateway and that
   `Packet.from_node_id` still identifies the sender. Adapt the query if those
   contracts change.
3. Test with a staging config and a verified copy of the live database. Check
   `/mqtt-nodes`, a listed gateway detail page, an unknown gateway (404), and
   the navigation alongside the standard pages. Compare the displayed counts
   with the staging database. Follow upstream's procedure for any migrations
   and keep the backup taken before migrating.
4. Deploy the tested branch through the site's normal upgrade process and
   repeat the HTTP checks. To remove only this feature, revert its commit and
   restart `meshview.service`. If rolling back a wider upgrade that migrated
   the database, follow upstream's rollback instructions for that schema.
