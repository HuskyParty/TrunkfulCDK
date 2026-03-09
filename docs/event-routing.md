# Event Routing: EventBridge Rules

## Event Types → Rules → Queues

```
                              ┌─────────────────────────────┐
                              │    TrunkfulEventBus          │
                              │    (custom EventBridge bus)   │
                              │                               │
                              │  source: trunkful.orders      │
                              └──────────────┬────────────────┘
                                             │
              All events enter the bus.      │      6 rules evaluate
              Each rule pattern-matches      │      INDEPENDENTLY and
              on detail-type.                │      in PARALLEL.
                                             │
       ┌─────────────┬──────────┬────────────┼────────────┬───────────┐
       │             │          │            │            │           │
       v             v          v            v            v           v
   ┌───────┐    ┌────────┐ ┌────────┐  ┌─────────┐ ┌─────────┐ ┌─────────┐
   │Rule 1 │    │Rule 2  │ │Rule 3  │  │Rule 4   │ │Rule 5   │ │Rule 6   │
   │       │    │        │ │        │  │         │ │         │ │         │
   │matches│    │matches │ │matches │  │matches  │ │matches  │ │matches  │
   │detail-│    │detail- │ │detail- │  │detail-  │ │detail-  │ │ALL from │
   │type:  │    │type:   │ │type:   │  │type:    │ │type:    │ │source:  │
   │       │    │        │ │        │  │         │ │         │ │trunkful │
   │Order  │    │Order   │ │Order   │  │Order    │ │Order    │ │.orders  │
   │Created│    │Created │ │Confirmed│ │Confirmed│ │Confirmed│ │         │
   │       │    │Inventory│ │        │  │         │ │Order    │ │         │
   │       │    │Received│ │        │  │         │ │Failed   │ │         │
   │       │    │Return  │ │        │  │         │ │Inventory│ │         │
   │       │    │Initiated│ │        │  │         │ │Low      │ │         │
   └───┬───┘    └───┬────┘ └───┬────┘  └────┬────┘ └────┬────┘ └────┬────┘
       │            │          │             │           │           │
       v            v          v             v           v           v
  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐
  │ order   │ │inventory │ │billing │ │fulfill-  │ │notific-  │ │Firehose │
  │ queue   │ │ queue    │ │ queue  │ │ment queue│ │ation     │ │→ S3     │
  │         │ │          │ │        │ │          │ │ queue    │ │→ Glue   │
  │ batch:1 │ │ batch:1  │ │batch:5 │ │ batch:5  │ │ batch:5  │ │→ Athena │
  └────┬────┘ └────┬─────┘ └───┬────┘ └────┬─────┘ └────┬─────┘ └─────────┘
       │           │           │            │            │
       v           v           v            v            v
  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ ┌──────────┐
  │ order   │ │inventory │ │billing │ │fulfill-  │ │notific-  │
  │ service │ │ service  │ │ lambda │ │ment      │ │ation     │
  │(durable)│ │(durable) │ │        │ │ lambda   │ │ lambda   │
  └─────────┘ └──────────┘ └────────┘ └──────────┘ └──────────┘
```

## Which Events Hit Which Queues (cross-reference)

```
                          ┌───────┬───────┬───────┬───────┬───────┬─────────┐
                          │Order  │Inven- │Billing│Fulfil-│Notif- │Firehose │
                          │Queue  │tory Q │Queue  │ment Q │ication│(all)    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  OrderCreated            │   *   │   *   │       │       │       │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  OrderValidated          │       │       │       │       │       │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  OrderReserved           │       │       │       │       │       │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  OrderConfirmed          │       │       │   *   │   *   │   *   │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  OrderFailed             │       │       │       │       │   *   │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  InventoryReceived       │       │   *   │       │       │       │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  InventoryAdjusted       │       │       │       │       │       │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  InventoryLow            │       │       │       │       │   *   │    *    │
  ────────────────────────┼───────┼───────┼───────┼───────┼───────┼─────────┤
  ReturnInitiated         │       │   *   │       │       │       │    *    │
  ────────────────────────┴───────┴───────┴───────┴───────┴───────┴─────────┘
```

Note: InventoryAdjusted is intentionally NOT routed to the inventory queue.
It is a notification event ("this happened"), not a command ("do this").
Routing it back would create an infinite feedback loop and double-count
adjustments. It flows only to Firehose (Rule 6) for analytics.

OrderCreated fans out to BOTH the order queue AND the inventory queue
simultaneously. The order service runs the saga while the inventory service
independently reserves stock. OrderConfirmed fans out to THREE queues in
parallel (billing, fulfillment, notification).
