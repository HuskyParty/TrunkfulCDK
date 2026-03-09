# Inventory Service: All Flows

## 4 Event Sources Feed the Inventory Queue

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  TrunkfulEventBus — Rule 2 (InventoryEventsToInventoryQueue)         │
  │  Matches: OrderCreated | InventoryReceived | ReturnInitiated         │
  └────────────────────────────────┬─────────────────────────────────────┘
                                   │
            Who emits these?       │
            ┌──────────────────────┼────────────────────────────────┐
            │                      │                                │
  ┌─────────┴──────────┐  ┌───────┴──────────┐  ┌──────────────────┴──────┐
  │  INTAKE LAMBDAS    │  │  WAREHOUSE       │  │  INVENTORY SERVICE      │
  │  (order-intake,    │  │  SCANNERS        │  │  ITSELF                 │
  │   pos-intake,      │  │  (direct         │  │  (emits Inventory-      │
  │   webhook-intake,  │  │   PutEvents      │  │   Adjusted after every  │
  │   admin-ingest)    │  │   via IAM role)  │  │   DDB update)           │
  │                    │  │                  │  │                          │
  │  emit:             │  │  emit:           │  │  emit:                   │
  │  OrderCreated      │  │  InventoryReceived│ │  InventoryAdjusted  ◄────── FEEDBACK
  │                    │  │  ReturnInitiated  │  │  InventoryLow            │
  └────────────────────┘  └──────────────────┘  └──────────────────────────┘
            │                      │                        │
            v                      v                        v
  ┌──────────────────────────────────────────────────────────────────────┐
  │                      inventory-queue                                  │
  │                      (batchSize: 1)                                   │
  │                                                                       │
  │  ┌──────────────┐ ┌────────────────┐ ┌────────────────┐              │
  │  │ OrderCreated │ │ Inventory-     │ │ Return-        │ ...          │
  │  │ Order #ABC   │ │ Received       │ │ Initiated      │              │
  │  │ 3 line items │ │ SKU-100 +500   │ │ SKU-200 +2     │              │
  │  └──────────────┘ └────────────────┘ └────────────────┘              │
  └──────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 v
                    ┌────────────────────────┐
                    │  INVENTORY SERVICE      │
                    │  (durable execution)    │
                    │                         │
                    │  Routes on detail-type  │
                    └────────────┬────────────┘
                                 │
                    ┌────────────┴────────────┐
                    │                         │
                    v                         v
             detail-type ==            detail-type ==
             "OrderCreated"            anything else
             (reservation flow)        (adjustment flow)
```

## Flow A: OrderCreated → Multi-Item Stock Reservation

```
  OrderCreated event arrives with an order containing 3 items:
  { orderId: "ABC-123", items: [ SKU-100 x2, SKU-200 x1, SKU-300 x5 ] }

  ┌────────────────────────────────────────────────────────────────────┐
  │  INVENTORY SERVICE — durable execution                             │
  │                                                                    │
  │  ┌─ ITEM 1: SKU-100 ────────────────────────────────────────────┐ │
  │  │                                                               │ │
  │  │  step: reserve-SKU100-DEFAULT                                 │ │
  │  │    DDB: ADD quantity = -2                                     │ │
  │  │    Key: pk=SKU#SKU-100, sk=WAREHOUSE#DEFAULT                  │ │
  │  │    Returns: newQty = 48                                       │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  step: emit-adjusted-SKU100-DEFAULT                           │ │
  │  │    emit InventoryAdjusted { sku: SKU-100, adjustedBy: -2,     │ │
  │  │                             newQuantity: 48,                  │ │
  │  │                             reason: "OrderCreated:ABC-123" }  │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  48 >= 10 → no InventoryLow                                  │ │
  │  └───────────────────────────────────────────────────────────────┘ │
  │                                                                    │
  │  ┌─ ITEM 2: SKU-200 ────────────────────────────────────────────┐ │
  │  │                                                               │ │
  │  │  step: reserve-SKU200-DEFAULT                                 │ │
  │  │    DDB: ADD quantity = -1                                     │ │
  │  │    Returns: newQty = 7                                        │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  step: emit-adjusted-SKU200-DEFAULT                           │ │
  │  │    emit InventoryAdjusted                                     │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  7 < 10 → LOW STOCK!                                         │ │
  │  │                                                               │ │
  │  │  step: emit-low-SKU200-DEFAULT                                │ │
  │  │    emit InventoryLow { sku: SKU-200, currentQuantity: 7,      │ │
  │  │                        threshold: 10 }                        │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  └───────────────────────────────────────────────────────────────┘ │
  │                                                                    │
  │  ┌─ ITEM 3: SKU-300 ────────────────────────────────────────────┐ │
  │  │                                                               │ │
  │  │  step: reserve-SKU300-DEFAULT                                 │ │
  │  │    DDB: ADD quantity = -5                                     │ │
  │  │    Returns: newQty = 120                                      │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  step: emit-adjusted-SKU300-DEFAULT                           │ │
  │  │    emit InventoryAdjusted                                     │ │
  │  │    CHECKPOINT ✓                                               │ │
  │  │                                                               │ │
  │  │  120 >= 10 → no InventoryLow                                 │ │
  │  └───────────────────────────────────────────────────────────────┘ │
  │                                                                    │
  │  return { orderId: "ABC-123", status: "reserved" }                 │
  └────────────────────────────────────────────────────────────────────┘

  Events emitted during this execution:
  ┌─────────────────────┬─────────────────────────────────────────────┐
  │ InventoryAdjusted   │  SKU-100, -2, qty=48  (→ Rule 6 only)     │
  │ InventoryAdjusted   │  SKU-200, -1, qty=7   (→ Rule 6 only)     │
  │ InventoryLow        │  SKU-200, qty=7       (→ Rule 5, Rule 6)  │
  │ InventoryAdjusted   │  SKU-300, -5, qty=120 (→ Rule 6 only)     │
  └─────────────────────┴─────────────────────────────────────────────┘
  Note: InventoryAdjusted goes to Firehose only (analytics).
  It is NOT routed back to the inventory queue (see feedback loop fix below).
```

## Flow B: Warehouse Receive / Return / Generic Adjustment

```
  Warehouse scanner sends InventoryReceived via direct PutEvents:
  { sku: "SKU-200", warehouseId: "WH-EAST", quantity: 500 }

  ┌────────────────────────────────────────────────────────────────────┐
  │  INVENTORY SERVICE — durable execution                             │
  │                                                                    │
  │  detail-type != "OrderCreated" → generic adjustment path           │
  │                                                                    │
  │  step: adjust-inventory                                            │
  │    DDB: ADD quantity = +500                                        │
  │    Key: pk=SKU#SKU-200, sk=WAREHOUSE#WH-EAST                      │
  │    Returns: newQty = 507                                           │
  │    CHECKPOINT ✓                                                    │
  │                                                                    │
  │  step: emit-adjusted                                               │
  │    emit InventoryAdjusted { sku: SKU-200, warehouseId: WH-EAST,   │
  │                             adjustedBy: +500, newQuantity: 507 }   │
  │    CHECKPOINT ✓                                                    │
  │                                                                    │
  │  507 >= 10 → no InventoryLow                                      │
  │                                                                    │
  │  return { sku: "SKU-200", newQuantity: 507 }                       │
  └────────────────────────────────────────────────────────────────────┘
```

## The Feedback Loop (and how we prevent it)

```
  ╔════════════════════════════════════════════════════════════════════╗
  ║  DESIGN NOTE: Why InventoryAdjusted is NOT in Rule 2              ║
  ╚════════════════════════════════════════════════════════════════════╝

  The inventory service emits InventoryAdjusted after every DDB write.
  If Rule 2 matched InventoryAdjusted, the event would route back into
  the inventory queue and create an infinite loop:

  OrderCreated ──> inventory-queue ──> inventory service
                                            │
                                            ├─ DDB: reserve stock
                                            │
                                            └─ emit InventoryAdjusted ─────┐
                                                                           │
       ┌───────────────────────────────────────────────────────────────────┘
       │
       v
  EventBridge Rule 2 matches InventoryAdjusted       ← WOULD re-enter
       │
       v
  inventory-queue ──> inventory service
                           │
                           │  step: adjust-inventory
                           │    DDB: ADD quantity = adjustedBy
                           │    DOUBLE-COUNT! Re-applies same delta.
                           │
                           │  step: emit-adjusted → InventoryAdjusted
                           │    → matches Rule 2 again → INFINITE LOOP ∞

  ╔════════════════════════════════════════════════════════════════════╗
  ║                                                                    ║
  ║  FIX (applied): InventoryAdjusted removed from Rule 2.            ║
  ║                                                                    ║
  ║  Rule 2 matches ONLY command events:                               ║
  ║    OrderCreated | InventoryReceived | ReturnInitiated              ║
  ║                                                                    ║
  ║  InventoryAdjusted is a NOTIFICATION ("this happened"), not a      ║
  ║  COMMAND ("do this"). It flows only to Rule 6 (Firehose/analytics).║
  ║                                                                    ║
  ╚════════════════════════════════════════════════════════════════════╝

  Correct flow after fix:

  OrderCreated ──> inventory-queue ──> inventory service
                                            │
                                            ├─ DDB: reserve stock
                                            │
                                            └─ emit InventoryAdjusted ─────┐
                                                                           │
                                                              Rule 2: NO MATCH
                                                              Rule 6: MATCH
                                                                           │
                                                                           v
                                                                    ┌───────────┐
                                                                    │ Firehose  │
                                                                    │ → S3      │
                                                                    │ (analytics│
                                                                    │  only)    │
                                                                    └───────────┘
```

## DynamoDB Inventory Table: Multi-Warehouse Layout

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  Inventory Table                                                  │
  │  Partition Key: pk (String)     Sort Key: sk (String)             │
  ├──────────────────┬─────────────────────┬────────┬────────────────┤
  │ pk               │ sk                  │quantity │ updatedAt      │
  ├──────────────────┼─────────────────────┼────────┼────────────────┤
  │ SKU#SKU-100      │ WAREHOUSE#DEFAULT   │   48   │ 2026-03-09T... │
  │ SKU#SKU-100      │ WAREHOUSE#WH-EAST   │  200   │ 2026-03-08T... │
  │ SKU#SKU-100      │ WAREHOUSE#WH-WEST   │  175   │ 2026-03-07T... │
  │ SKU#SKU-200      │ WAREHOUSE#DEFAULT   │    7   │ 2026-03-09T... │ ← LOW
  │ SKU#SKU-200      │ WAREHOUSE#WH-EAST   │  507   │ 2026-03-09T... │
  │ SKU#SKU-300      │ WAREHOUSE#DEFAULT   │  120   │ 2026-03-09T... │
  └──────────────────┴─────────────────────┴────────┴────────────────┘

  Key design:
  - pk = SKU#{sku}           → partition by product
  - sk = WAREHOUSE#{whId}    → sort by warehouse location
  - Query pk = "SKU#SKU-100" → returns stock across ALL warehouses
  - ADD quantity :qty         → atomic increment/decrement (no read-then-write race)
  - ReturnValues: ALL_NEW     → get new quantity in same round-trip for threshold check
```

## Durable Replay: Crash Mid-Order with 3 Items

```
  Order with 3 items, Lambda crashes after item 2:

  ┌──────────────────────────────────────────────────────────────────┐
  │  INVOCATION 1                                                     │
  │                                                                   │
  │  Item 1: SKU-100                                                  │
  │    step: reserve-SKU100-DEFAULT  → DDB -2  → CHECKPOINT ✓        │
  │    step: emit-adjusted-SKU100    → emitted  → CHECKPOINT ✓        │
  │                                                                   │
  │  Item 2: SKU-200                                                  │
  │    step: reserve-SKU200-DEFAULT  → DDB -1  → CHECKPOINT ✓        │
  │    step: emit-adjusted-SKU200    → emitted  → CHECKPOINT ✓        │
  │    step: emit-low-SKU200         → emitted  → CHECKPOINT ✓        │
  │                                                                   │
  │  Item 3: SKU-300                                                  │
  │    step: reserve-SKU300-DEFAULT  → ██ LAMBDA OOM CRASH ██        │
  │                                                                   │
  └──────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────┐
  │  INVOCATION 2 (replay)                                            │
  │                                                                   │
  │  Item 1: SKU-100                                                  │
  │    step: reserve-SKU100-DEFAULT  → SKIP (checkpoint: qty=48)      │
  │    step: emit-adjusted-SKU100    → SKIP (checkpoint: emitted)     │
  │                                    ^^^^                           │
  │                          NOT re-emitted. No duplicate event.      │
  │                          NOT re-decremented. Stock stays at 48.   │
  │                                                                   │
  │  Item 2: SKU-200                                                  │
  │    step: reserve-SKU200-DEFAULT  → SKIP (checkpoint: qty=7)       │
  │    step: emit-adjusted-SKU200    → SKIP                           │
  │    step: emit-low-SKU200         → SKIP                           │
  │                                                                   │
  │  Item 3: SKU-300                                                  │
  │    step: reserve-SKU300-DEFAULT  → RUNS (no checkpoint)           │
  │                                    DDB -5, newQty = 120           │
  │                                    CHECKPOINT ✓                   │
  │    step: emit-adjusted-SKU300    → RUNS                           │
  │                                    CHECKPOINT ✓                   │
  │                                                                   │
  │  return { orderId: "ABC-123", status: "reserved" }                │
  └──────────────────────────────────────────────────────────────────┘

  WITHOUT durable execution, invocation 2 would have:
  - Decremented SKU-100 AGAIN (double-reserved, stock 46 instead of 48)
  - Emitted InventoryAdjusted AGAIN (duplicate events downstream)
  - Sent ANOTHER InventoryLow for SKU-200 (duplicate alert)
```

## Full Lifecycle: All Inventory Mutations

```
  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
  │ RECEIVE  │   │ ORDER    │   │ PAYMENT  │   │ RETURN   │   │ MANUAL   │
  │          │   │ PLACED   │   │ FAILED   │   │          │   │ ADJUST   │
  └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘   └────┬─────┘
       │              │              │              │              │
       v              v              v              v              v
  Inventory-     Order-         Inventory-     Return-        Inventory-
  Received       Created        Release-       Initiated      Adjusted
  qty: +500      qty: -N        Requested      qty: +N        qty: +/-N
       │              │         (from order     │              │
       │              │          service         │              │
       │              │          compensation)   │              │
       v              v              v           v              v
  ┌──────────────────────────────────────────────────────────────────┐
  │                    DynamoDB Inventory Table                       │
  │                    (atomic ADD operations)                        │
  │                                                                   │
  │    500 ──────────────────────────────────────────────── 507       │
  │     │    receive         reserve      return                      │
  │     │    +500   ┌───┐    -2     ┌──┐  +2     ┌──┐                │
  │     ▓▓▓▓▓▓▓▓▓▓▓│   │▓▓▓▓▓▓▓▓▓▓│  │▓▓▓▓▓▓▓▓│  │▓▓▓▓▓▓▓        │
  │     0           │   │          │  │         │  │                  │
  │                 500  498       498 507      507                   │
  │                                                                   │
  └──────────────────────────────────────────────────────────────────┘
                         │                  │
                    if qty < 10        if qty >= 10
                         │                  │
                         v                  v
                  ┌─────────────┐    (no alert)
                  │InventoryLow │
                  │ → Rule 5    │
                  │ → notif-    │
                  │   ication   │
                  │   queue     │
                  └─────────────┘
```
