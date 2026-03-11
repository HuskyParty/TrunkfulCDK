# Inventory Service: All Flows

## 3 Event Sources Feed the Inventory Queue

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  TrunkfulEventBus                                                    │
  │  Rule 2: InventoryEvents→InventoryQueue                             │
  │  Matches: OrderCreated | InventoryReceived | ReturnInitiated         │
  │  (InventoryAdjusted deliberately EXCLUDED — see feedback loop below) │
  └────────────────────────────────┬─────────────────────────────────────┘
                                   │
            Who emits these?       │
            ┌──────────────────────┴────────────────────────┐
            │                                               │
  ┌─────────┴──────────┐                          ┌────────┴─────────┐
  │  INTAKE LAMBDAS    │                          │  WAREHOUSE       │
  │  (order-intake,    │                          │  SCANNERS        │
  │   pos-intake,      │                          │  (direct         │
  │   webhook-intake,  │                          │   PutEvents      │
  │   admin-ingest)    │                          │   via IAM role)  │
  │                    │                          │                  │
  │  emit:             │                          │  emit:           │
  │  OrderCreated      │                          │  InventoryReceived│
  └─────────┬──────────┘                          │  ReturnInitiated │
            │                                     └────────┬─────────┘
            │                                              │
            v  OrderCreated                                v  InventoryReceived / ReturnInitiated
  ┌──────────────────────────────────────────────────────────────────────┐
  │                      inventory-queue                                  │
  │                      (batchSize: 1)                                   │
  │                                                                       │
  │  ┌──────────────┐ ┌────────────────┐ ┌────────────────┐              │
  │  │ OrderCreated │ │ Inventory-     │ │ Return-        │              │
  │  │ Order #ABC   │ │ Received       │ │ Initiated      │              │
  │  │ 3 line items │ │ SKU-100 +500   │ │ SKU-200 +2     │              │
  │  └──────────────┘ └────────────────┘ └────────────────┘              │
  └──────────────────────────────┬────────────────────────────────────────┘
                                 │ SQS message
                                 v
                    ┌────────────────────────┐
                    │  trigger lambda          │
                    │  Extracts EventBridge    │
                    │  envelope, calls         │
                    │  StartExecution          │
                    └────────────┬─────────────┘
                                 │ StartExecution
                                 v
  ┌──────────────────────────────────────────────────────────────────────┐
  │  INVENTORY WORKFLOW  (Step Functions state machine)                   │
  │                                                                       │
  │  NormalizeInput (Pass)                                                │
  │    ─ extracts detail-type → detailType, detail → detail              │
  │         │                                                             │
  │         v                                                             │
  │  Choice: detailType == "OrderCreated"?                                │
  │         │                          │                                  │
  │         v YES                      v NO (otherwise)                   │
  │  MapOrderItems (Map)         ExtractGenericDetail (Pass)             │
  │    iterates detail.items       inputPath: $.detail                   │
  │    ┌──────────────────┐            │                                  │
  │    │ ProcessItem      │            v                                  │
  │    │ Lambda (per item)│      ProcessGenericItem Lambda                │
  │    └──────────────────┘                                               │
  └──────────────────────────────────────────────────────────────────────┘

  Inventory service emits after each DDB write (NOT routed back to queue):
    InventoryAdjusted  → Rule 6: AllEvents→Firehose only
    InventoryLow       → Rule 5: NotificationEvents→NotificationQueue
                          + Rule 6: AllEvents→Firehose
```

## Flow A: OrderCreated → Multi-Item Stock Reservation (Map State)

```
  OrderCreated event arrives with an order containing 3 items:
  { orderId: "ABC-123", items: [ SKU-100 x2, SKU-200 x1, SKU-300 x5 ] }

  ┌────────────────────────────────────────────────────────────────────┐
  │  INVENTORY WORKFLOW — Step Functions                               │
  │                                                                    │
  │  NormalizeInput (Pass state)                                       │
  │    Extract detail-type → detailType, detail → detail               │
  │                                                                    │
  │  Choice: detailType == "OrderCreated"? → YES                       │
  │                                                                    │
  │  MapOrderItems (Map state, iterates $.detail.items)                │
  │  ┌────────────────────────────────────────────────────────────────┐│
  │  │  Each iteration receives:                                      ││
  │  │  { sku, quantity, orderId, isReservation: true }               ││
  │  │                                                                ││
  │  │  ┌─ ITEM 1: SKU-100 ────────────────────────────────────────┐ ││
  │  │  │  ProcessItem Lambda:                                      │ ││
  │  │  │    Idempotency check: INV#ABC-123#SKU-100#DEFAULT         │ ││
  │  │  │    DDB: ADD quantity = -2                                 │ ││
  │  │  │    Key: pk=SKU#SKU-100, sk=WAREHOUSE#DEFAULT              │ ││
  │  │  │    Returns: newQty = 48                                   │ ││
  │  │  │    emit InventoryAdjusted { adjustedBy: -2, qty: 48 }     │ ││
  │  │  │    48 >= 10 → no InventoryLow                             │ ││
  │  │  └───────────────────────────────────────────────────────────┘ ││
  │  │                                                                ││
  │  │  ┌─ ITEM 2: SKU-200 ────────────────────────────────────────┐ ││
  │  │  │  ProcessItem Lambda:                                      │ ││
  │  │  │    Idempotency check: INV#ABC-123#SKU-200#DEFAULT         │ ││
  │  │  │    DDB: ADD quantity = -1                                 │ ││
  │  │  │    Returns: newQty = 7                                    │ ││
  │  │  │    emit InventoryAdjusted                                 │ ││
  │  │  │    7 < 10 → LOW STOCK!                                   │ ││
  │  │  │    emit InventoryLow { sku: SKU-200, qty: 7 }            │ ││
  │  │  └───────────────────────────────────────────────────────────┘ ││
  │  │                                                                ││
  │  │  ┌─ ITEM 3: SKU-300 ────────────────────────────────────────┐ ││
  │  │  │  ProcessItem Lambda:                                      │ ││
  │  │  │    Idempotency check: INV#ABC-123#SKU-300#DEFAULT         │ ││
  │  │  │    DDB: ADD quantity = -5                                 │ ││
  │  │  │    Returns: newQty = 120                                  │ ││
  │  │  │    emit InventoryAdjusted                                 │ ││
  │  │  │    120 >= 10 → no InventoryLow                            │ ││
  │  │  └───────────────────────────────────────────────────────────┘ ││
  │  └────────────────────────────────────────────────────────────────┘│
  │                                                                    │
  │  Execution succeeds                                                │
  └────────────────────────────────────────────────────────────────────┘

  Events emitted during this execution:
  ┌─────────────────────┬──────────────────────────────────────────────────────────┐
  │ InventoryAdjusted   │  SKU-100, -2, qty=48                                    │
  │                     │    → Rule 6: AllEvents→Firehose only                     │
  │ InventoryAdjusted   │  SKU-200, -1, qty=7                                     │
  │                     │    → Rule 6: AllEvents→Firehose only                     │
  │ InventoryLow        │  SKU-200, qty=7                                          │
  │                     │    → Rule 5: NotificationEvents→NotificationQueue        │
  │                     │    → Rule 6: AllEvents→Firehose                          │
  │ InventoryAdjusted   │  SKU-300, -5, qty=120                                   │
  │                     │    → Rule 6: AllEvents→Firehose only                     │
  └─────────────────────┴──────────────────────────────────────────────────────────┘
  Note: InventoryAdjusted is NOT in Rule 2 (InventoryEvents→InventoryQueue).
  It goes to Firehose only. See "Feedback Loop" section below.
```

## Idempotency: Safe Retries in Map State

```
  ╔════════════════════════════════════════════════════════════════════╗
  ║  DESIGN NOTE: Why the ProcessItem Lambda has an idempotency check ║
  ╚════════════════════════════════════════════════════════════════════╝

  DynamoDB ADD operations are NOT idempotent — running the same
  adjustment twice would double-count. If the Map state partially
  succeeds (items 1-2 reserved) then fails on item 3, SQS retries
  the message, starting a NEW state machine execution that re-processes
  all items.

  To prevent double-adjustment, each ProcessItem invocation writes
  an idempotency key to the Idempotency table via a DynamoDB
  conditional PutItem (attribute_not_exists) before adjusting:

    Key: INV#{orderId}#{sku}#{warehouseId}
    TTL: 24 hours

  On retry:
    Item 1 → idempotency key exists → SKIP (no double-decrement)
    Item 2 → idempotency key exists → SKIP
    Item 3 → idempotency key missing → RUNS (first successful attempt)

  This provides replay safety using application-level idempotency
  within the Step Functions Map state.
```

## Flow B: Warehouse Receive / Return / Generic Adjustment

```
  Warehouse scanner sends InventoryReceived via direct PutEvents
  → Rule 2: InventoryEvents→InventoryQueue
  → inventory-queue → trigger lambda → StartExecution

  Payload: { sku: "SKU-200", warehouseId: "WH-EAST", quantity: 500 }

  ┌────────────────────────────────────────────────────────────────────┐
  │  INVENTORY WORKFLOW — Step Functions state machine                  │
  │                                                                    │
  │  NormalizeInput (Pass state)                                       │
  │    Extract detail-type → detailType, detail → detail               │
  │                                                                    │
  │  Choice: detailType == "OrderCreated"? → NO (otherwise)            │
  │                                                                    │
  │  ExtractGenericDetail (Pass state, inputPath: $.detail)            │
  │    Passes { sku: "SKU-200", warehouseId: "WH-EAST", qty: 500 }    │
  │                                                                    │
  │  ProcessGenericItem (Lambda)                                       │
  │    DDB: ADD quantity = +500                                        │
  │    Key: pk=SKU#SKU-200, sk=WAREHOUSE#WH-EAST                      │
  │    Returns: newQty = 507                                           │
  │    emit InventoryAdjusted { adjustedBy: +500, newQuantity: 507 }   │
  │    507 >= 10 → no InventoryLow                                     │
  │                                                                    │
  │  Execution succeeds: { sku: "SKU-200", newQuantity: 507 }          │
  └────────────────────────────────────────────────────────────────────┘
```

## The Feedback Loop (and how we prevent it)

```
  ╔═══════════════════════════════════════════════════════════════════════════╗
  ║  DESIGN NOTE: Why InventoryAdjusted is NOT in                            ║
  ║  Rule 2: InventoryEvents→InventoryQueue                                  ║
  ╚═══════════════════════════════════════════════════════════════════════════╝

  The inventory service emits InventoryAdjusted after every DDB write.
  If Rule 2 matched InventoryAdjusted, the event would route back into
  the inventory queue and create an infinite loop:

  OrderCreated ──> inventory-queue ──> trigger lambda ──> Step Functions
                                                               │
                                                               ├─ DDB: reserve stock
                                                               │
                                                               └─ emit InventoryAdjusted ──┐
                                                                                           │
       ┌───────────────────────────────────────────────────────────────────────────────────┘
       │
       v
  Rule 2 (InventoryEvents→InventoryQueue)
  matches InventoryAdjusted  ← WOULD re-enter
       │
       v
  inventory-queue ──> trigger lambda ──> Step Functions
                                              │
                                              │  ProcessItem Lambda
                                              │    DDB: ADD quantity = adjustedBy
                                              │    DOUBLE-COUNT! Re-applies same delta.
                                              │
                                              │  emit InventoryAdjusted
                                              │    → matches Rule 2 again → INFINITE LOOP

  ╔═══════════════════════════════════════════════════════════════════════════╗
  ║                                                                          ║
  ║  FIX (applied): InventoryAdjusted excluded from                          ║
  ║  Rule 2: InventoryEvents→InventoryQueue.                                ║
  ║                                                                          ║
  ║  Rule 2 matches ONLY command events:                                     ║
  ║    OrderCreated | InventoryReceived | ReturnInitiated                    ║
  ║                                                                          ║
  ║  InventoryAdjusted is a NOTIFICATION ("this happened"), not a            ║
  ║  COMMAND ("do this"). It flows only to                                   ║
  ║  Rule 6: AllEvents→Firehose.                                            ║
  ║                                                                          ║
  ╚═══════════════════════════════════════════════════════════════════════════╝

  Correct flow after fix:

  OrderCreated ──> inventory-queue ──> trigger lambda ──> Step Functions
                                                               │
                                                               ├─ DDB: reserve stock
                                                               │
                                                               └─ emit InventoryAdjusted ──┐
                                                                                           │
                                       Rule 2 (InventoryEvents→InventoryQueue): NO MATCH   │
                                       Rule 6 (AllEvents→Firehose):              MATCH ◄───┘
                                                                                  │
                                                                                  v
                                                                           ┌───────────┐
                                                                           │ Firehose  │
                                                                           │ → S3      │
                                                                           │(analytics)│
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
       │              │          saga            │              │
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
                  ┌───────────────────────────────────────┐  (no alert)
                  │ InventoryLow                          │
                  │  → Rule 5: NotificationEvents→        │
                  │    NotificationQueue                   │
                  │  → Rule 6: AllEvents→Firehose          │
                  └───────────────────────────────────────┘
```

## EventBridge Rule Reference

```
  Rule 1: OrderCreated→OrderQueue
    Matches: OrderCreated
    Target:  order-queue

  Rule 2: InventoryEvents→InventoryQueue
    Matches: OrderCreated | InventoryReceived | ReturnInitiated
    Target:  inventory-queue
    NOTE:    InventoryAdjusted deliberately excluded (feedback loop)

  Rule 3: OrderConfirmed→BillingQueue
    Matches: OrderConfirmed
    Target:  billing-queue

  Rule 4: OrderConfirmed→FulfillmentQueue
    Matches: OrderConfirmed
    Target:  fulfillment-queue

  Rule 5: NotificationEvents→NotificationQueue
    Matches: OrderConfirmed | OrderFailed | InventoryLow
    Target:  notification-queue

  Rule 6: AllEvents→Firehose
    Matches: all events on TrunkfulEventBus (source: trunkful.orders)
    Target:  Kinesis Firehose → S3 (analytics)
```
