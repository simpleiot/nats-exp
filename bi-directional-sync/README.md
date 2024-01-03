# Syncing streams between hub and leaf nodes

output of this program:

```
# [cbrake@quark nats-exp]$ go run ./bi-directional-sync/

       Create stream on hub and source to leaf

=====================================================

- Create stream: server:hub domain:hub stream:NODES-HUB subject:n.hub.>
- Publish: hub -> n.hub.123.value -> 12 13 14 15
- Stream count: server:hub domain:hub stream:NODES-HUB count:4
- Stream count: server:leaf domain:hub stream:NODES-HUB count:4
- Source stream: server:leaf domain:leaf stream:NODES-HUB source-domain:hub subject:n.hub.>
- # Stream count: server:leaf domain:leaf stream:NODES-HUB count:4
         Create stream on leaf and source to hub
  =====================================================
- Create stream: server:leaf domain:leaf stream:NODES-LEAF subject:n.leaf.>
- Publish: leaf -> n.leaf.456.value -> 22 23 24 25 26
- Source stream: server:hub domain:hub stream:NODES-LEAF source-domain:leaf subject:n.leaf.>
- # Stream count: server:hub domain:hub stream:NODES-LEAF count:5
         Shut down leaf node
  =====================================================
- Stream count: server:hub domain:hub stream:NODES-LEAF count:5
- Publish: hub -> n.hub.123.value -> 16 17 18 19
- # Stream count: server:hub domain:hub stream:NODES-HUB count:8
         Start up leaf node
  =====================================================
- Stream count: server:leaf domain:leaf stream:NODES-HUB count:8
- Publish: hub -> n.hub.123.value -> 20 21 22 23
- # Stream count: server:leaf domain:leaf stream:NODES-HUB count:12
         Publish more messages to leaf
  =====================================================
- Publish: leaf -> n.leaf.456.value -> 27 28 29 30 31
- Stream count: server:leaf domain:leaf stream:NODES-LEAF count:10
- Stream count: server:leaf domain:leaf stream:NODES-LEAF count:10
- Stream count: server:hub domain:hub stream:NODES-LEAF count:10
```
