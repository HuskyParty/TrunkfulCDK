# Step Functions Deep Dive

## 1. Overview: Why Step Functions?

Trunkful uses two AWS Step Functions state machines to orchestrate multi-step
workflows that would otherwise require hand-rolled retry loops, status tracking,
and compensation logic inside long-running Lambdas.

**What we gained by switching from "durable Lambda chains" to Step Functions:**

```
  ┌──────────────────────────┬──────────────────────────────────────────────┐
  │  Capability              │  How Step Functions delivers it              │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  Visual execution history│  Every state transition recorded with        │
  │                          │  timestamps, inputs, outputs, and errors.    │
  │                          │  Viewable in the AWS console per-execution.  │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  Native retry            │  addRetry() on each state: 2 attempts,      │
  │                          │  exponential backoff (1s * 2x), handled by  │
  │                          │  the runtime -- not our code.               │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  Catch-based compensation│  addCatch() routes errors to compensation   │
  │                          │  states automatically. No try/catch wiring  │
  │                          │  across Lambda invocations.                 │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  Map state for parallel  │  sfn.Map iterates over order items in       │
  │  item processing         │  parallel, each item gets its own Lambda    │
  │                          │  invocation and independent retry.          │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  Timeout at SM level     │  5-minute execution timeout ensures no      │
  │                          │  zombie workflows.                          │
  └──────────────────────────┴──────────────────────────────────────────────┘
```

**The two state machines:**

```
  ┌───────────────────────────────────┐    ┌───────────────────────────────────┐
  │  {stage}-trunkful-order-saga      │    │  {stage}-trunkful-inventory-      │
  │                                   │    │  workflow                         │
  │  Purpose: Validate → Reserve →    │    │  Purpose: Adjust inventory per    │
  │  Pay → Confirm an order, with     │    │  item for OrderCreated events,    │
  │  saga compensation on failure.    │    │  or handle generic inventory      │
  │                                   │    │  events (received / returned).    │
  │  Trigger: SQS order-queue         │    │  Trigger: SQS inventory-queue     │
  │    via Rule 1: OrderCreated→      │    │    via Rule 2: InventoryEvents→   │
  │    OrderQueue                     │    │    InventoryQueue                 │
  └───────────────────────────────────┘    └───────────────────────────────────┘
```

---

## 2. Order Saga State Machine

### Full ASCII Diagram

```
  ┌────────────────────────────────────────────────────────────────────────────┐
  │  ORDER SAGA STATE MACHINE: {stage}-trunkful-order-saga                    │
  │  Timeout: 5 minutes    Tracing: enabled                                   │
  │                                                                            │
  │                                                                            │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  ValidateOrder (LambdaInvoke)                    │                       │
  │  │  fn: {stage}-trunkful-order-validate             │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │  retry: 2 attempts, backoff 2x, interval 1s     │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - Validates orderId exists                    │                       │
  │  │    - Validates items array non-empty             │                       │
  │  │    - Validates totalAmount > 0                   │                       │
  │  │    - DDB UpdateItem → status: VALIDATING        │                       │
  │  │    - PutEvents → OrderValidated                 │                       │
  │  │      {orderId}                                  │                       │
  │  │                                                  │                       │
  │  │  Catch → MarkFailedEarly (pre-reservation)      │                       │
  │  │          resultPath: $.error                     │                       │
  │  └──────────────────────┬───────────────────────────┘                       │
  │                         │ OK                                                │
  │                         v                                                   │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  ReserveInventory (LambdaInvoke)                 │                       │
  │  │  fn: {stage}-trunkful-order-reserve              │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │  retry: 2 attempts, backoff 2x, interval 1s     │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - DDB UpdateItem → status: RESERVED          │                       │
  │  │    - PutEvents → OrderReserved                  │                       │
  │  │      {orderId, items}                           │                       │
  │  │                                                  │                       │
  │  │  Catch → MarkFailedEarly (pre-reservation)      │                       │
  │  │          resultPath: $.error                     │                       │
  │  └──────────────────────┬───────────────────────────┘                       │
  │                         │ OK                                                │
  │                         v                                                   │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  ProcessPayment (LambdaInvoke)                   │                       │
  │  │  fn: {stage}-trunkful-order-payment              │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: '$'  *** REPLACES ENTIRE STATE ***  │                       │
  │  │  retry: 2 attempts, backoff 2x, interval 1s     │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - checkCircuit('payment')                    │                       │
  │  │      ├─ OPEN → throw immediately (fail-fast)    │                       │
  │  │      └─ CLOSED/HALF_OPEN → proceed              │                       │
  │  │    - processPayment(orderId, totalAmount)       │                       │
  │  │      ├─ success → recordSuccess('payment')      │                       │
  │  │      │   return { ...order, transactionId }     │                       │
  │  │      └─ failure → recordFailure('payment')      │                       │
  │  │          throw Error                             │                       │
  │  │                                                  │                       │
  │  │  Catch → CompensateRelease (post-reservation)   │                       │
  │  │          resultPath: $.error                     │                       │
  │  └──────────────────────┬───────────────────────────┘                       │
  │                         │ OK                                                │
  │                         v                                                   │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  ConfirmOrder (LambdaInvoke)                     │                       │
  │  │  fn: {stage}-trunkful-order-confirm              │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │  retry: 2 attempts, backoff 2x, interval 1s     │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - DDB UpdateItem → status: CONFIRMED         │                       │
  │  │      also stores transactionId                  │                       │
  │  │    - PutEvents → OrderConfirmed                 │                       │
  │  │      {orderId, customerId, totalAmount,         │                       │
  │  │       transactionId}                            │                       │
  │  │                                                  │                       │
  │  │  OrderConfirmed fans out via EventBridge:       │                       │
  │  │    Rule 3: OrderConfirmed→BillingQueue          │                       │
  │  │    Rule 4: OrderConfirmed→FulfillmentQueue      │                       │
  │  │    Rule 5: NotificationEvents→NotificationQueue │                       │
  │  │    Rule 6: AllEvents→Firehose                   │                       │
  │  │                                                  │                       │
  │  │  Catch → CompensateRelease (post-reservation)   │                       │
  │  │          resultPath: $.error                     │                       │
  │  └─────────────────────────────────────────────────┘                       │
  │                                                                            │
  │                                                                            │
  │  ══════════════ COMPENSATION / ERROR PATHS ═══════════════                 │
  │                                                                            │
  │                                                                            │
  │  PATH A: Pre-reservation error (Validate or Reserve fails)                 │
  │  ─────────────────────────────────────────────────────                      │
  │                                                                            │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  MarkFailedEarly (LambdaInvoke)                  │                       │
  │  │  fn: {stage}-trunkful-order-mark-failed          │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - Reads $.error.Cause or $.error.Error        │                       │
  │  │    - DDB UpdateItem → status: FAILED            │                       │
  │  │    - PutEvents → OrderFailed                    │                       │
  │  │      {orderId, reason}                          │                       │
  │  │                                                  │                       │
  │  │  OrderFailed fans out via EventBridge:          │                       │
  │  │    Rule 5: NotificationEvents→NotificationQueue │                       │
  │  │    Rule 6: AllEvents→Firehose                   │                       │
  │  └─────────────────────────────────────────────────┘                       │
  │                                                                            │
  │                                                                            │
  │  PATH B: Post-reservation error (Payment or Confirm fails)                 │
  │  ─────────────────────────────────────────────────────                      │
  │                                                                            │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  CompensateRelease (LambdaInvoke)                │                       │
  │  │  fn: {stage}-trunkful-order-release              │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - PutEvents → InventoryReleaseRequested      │                       │
  │  │      {orderId, items}                           │                       │
  │  └──────────────────────┬───────────────────────────┘                       │
  │                         │                                                   │
  │                         v                                                   │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  MarkFailedAfterComp (LambdaInvoke)              │                       │
  │  │  fn: {stage}-trunkful-order-mark-failed          │                       │
  │  │  payloadResponseOnly: true                       │                       │
  │  │  resultPath: DISCARD                             │                       │
  │  │                                                  │                       │
  │  │  Lambda logic:                                   │                       │
  │  │    - Same as MarkFailedEarly (same Lambda)      │                       │
  │  │    - DDB UpdateItem → status: FAILED            │                       │
  │  │    - PutEvents → OrderFailed                    │                       │
  │  │      {orderId, reason}                          │                       │
  │  └─────────────────────────────────────────────────┘                       │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘
```

### Catch Wiring Summary

```
  ┌───────────────────┐         ┌───────────────────────┐
  │  ValidateOrder    │──Catch─>│  MarkFailedEarly      │
  │  ReserveInventory │──Catch─>│  (no compensation     │
  │                   │         │   needed -- nothing    │
  │  (pre-reservation │         │   reserved yet)       │
  │   errors)         │         └───────────────────────┘
  └───────────────────┘

  ┌───────────────────┐         ┌───────────────────────┐     ┌──────────────────┐
  │  ProcessPayment   │──Catch─>│  CompensateRelease    │────>│MarkFailedAfterComp│
  │  ConfirmOrder     │──Catch─>│  (release inventory   │     │(DDB→FAILED,      │
  │                   │         │   that was reserved)  │     │ emit OrderFailed) │
  │  (post-reservation│         └───────────────────────┘     └──────────────────┘
  │   errors)         │
  └───────────────────┘
```

### resultPath Configuration

```
  ┌───────────────────┬────────────────┬──────────────────────────────────────┐
  │  State            │  resultPath    │  Why                                 │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  ValidateOrder    │  DISCARD       │  Returns the same order object it    │
  │                   │                │  received. No new data needed by     │
  │                   │                │  downstream states.                  │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  ReserveInventory │  DISCARD       │  Same -- original order object       │
  │                   │                │  passes through unchanged.           │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  ProcessPayment   │  '$'           │  REPLACES the entire state input     │
  │                   │  (root)        │  with the Lambda return value.       │
  │                   │                │  The Lambda returns:                 │
  │                   │                │    { ...order, transactionId }       │
  │                   │                │  This is how transactionId flows     │
  │                   │                │  to ConfirmOrder.                    │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  ConfirmOrder     │  DISCARD       │  Terminal happy-path state. Its      │
  │                   │                │  return value is not consumed.       │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  All Catch states │  DISCARD       │  MarkFailed* / CompensateRelease    │
  │                   │                │  do not produce output that later    │
  │                   │                │  states need.                       │
  ├───────────────────┼────────────────┼──────────────────────────────────────┤
  │  addCatch()       │  '$.error'     │  Error info (Error, Cause) is       │
  │  (on each step)   │                │  placed at $.error so MarkFailed    │
  │                   │                │  can read event.error.Cause.        │
  └───────────────────┴────────────────┴──────────────────────────────────────┘
```

**Why ProcessPayment uses `resultPath: '$'`:** Every other step uses DISCARD
because they just do side effects (DDB updates, event emissions) and return
the same order object the Lambda received. ProcessPayment is different -- it
adds a `transactionId` field that ConfirmOrder needs to persist alongside the
order status. By setting `resultPath: '$'`, the Lambda's return value
`{ ...order, transactionId: 'txn_...' }` replaces the entire state, so
ConfirmOrder receives the enriched object and can write the transactionId to
DynamoDB and include it in the OrderConfirmed event.

### DDB Status Updates and Events Per Step

```
  STEP                  DDB STATUS UPDATE        EVENT EMITTED
  ═══════════════════   ════════════════════     ═══════════════════════
  ValidateOrder         PENDING → VALIDATING     OrderValidated
  ReserveInventory      VALIDATING → RESERVED    OrderReserved
  ProcessPayment        (no DDB status update)   (no event -- circuit
                                                  breaker state updated
                                                  in DDB separately)
  ConfirmOrder          RESERVED → CONFIRMED     OrderConfirmed
                        + stores transactionId    (fans out to billing,
                                                   fulfillment, notif)

  MarkFailed*           → FAILED                 OrderFailed
  CompensateRelease     (no DDB update)          InventoryReleaseRequested
```

---

## 3. Inventory Workflow State Machine

### Full ASCII Diagram

```
  ┌────────────────────────────────────────────────────────────────────────────┐
  │  INVENTORY WORKFLOW STATE MACHINE: {stage}-trunkful-inventory-workflow     │
  │  Timeout: 5 minutes    Tracing: enabled                                   │
  │                                                                            │
  │  Input: full EventBridge envelope                                          │
  │    { "detail-type": "OrderCreated", "detail": { orderId, items, ... } }   │
  │                                                                            │
  │                                                                            │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  NormalizeInput (Pass state)                     │                       │
  │  │                                                  │                       │
  │  │  Problem: EventBridge uses "detail-type" (with   │                       │
  │  │  hyphen) which is not a valid JSONPath field     │                       │
  │  │  accessor without bracket notation.             │                       │
  │  │                                                  │                       │
  │  │  parameters:                                     │                       │
  │  │    detailType.$  = $['detail-type']              │                       │
  │  │    detail.$      = $.detail                      │                       │
  │  │                                                  │                       │
  │  │  Output:                                         │                       │
  │  │    { "detailType": "OrderCreated",               │                       │
  │  │      "detail": { orderId, items, ... } }        │                       │
  │  └──────────────────────┬───────────────────────────┘                       │
  │                         │                                                   │
  │                         v                                                   │
  │  ┌─────────────────────────────────────────────────┐                       │
  │  │  InventoryEventType (Choice state)               │                       │
  │  │                                                  │                       │
  │  │  Condition: $.detailType == "OrderCreated"       │                       │
  │  │    ├─ YES ─────────────────────────┐              │                       │
  │  │    └─ OTHERWISE (default) ─────────┼──┐           │                       │
  │  └────────────────────────────────────┘  │           │                       │
  │                         │                │                                  │
  │              ┌──────────┘                │                                  │
  │              │                           │                                  │
  │              v                           v                                  │
  │                                                                            │
  │  ═══ OrderCreated Path ═══    ═══ Generic Path ═══                         │
  │  (InventoryReceived,                                                       │
  │   ReturnInitiated, etc.)                                                   │
  │                                                                            │
  │  ┌──────────────────────┐     ┌──────────────────────┐                     │
  │  │  MapOrderItems       │     │  ExtractGenericDetail │                     │
  │  │  (Map state)         │     │  (Pass state)         │                     │
  │  │                      │     │                       │                     │
  │  │  itemsPath:          │     │  inputPath: $.detail  │                     │
  │  │    $.detail.items    │     │                       │                     │
  │  │                      │     │  Strips the envelope  │                     │
  │  │  itemSelector:       │     │  so the Lambda gets   │                     │
  │  │    sku    = item.sku │     │  the detail directly: │                     │
  │  │    quantity           │     │  { sku, quantity,     │                     │
  │  │         = item.qty   │     │    warehouseId }      │                     │
  │  │    orderId            │     └───────────┬──────────┘                     │
  │  │         = detail.    │                  │                                │
  │  │           orderId    │                  v                                │
  │  │    isReservation     │     ┌──────────────────────┐                     │
  │  │         = true       │     │  ProcessGenericItem   │                     │
  │  │                      │     │  (LambdaInvoke)       │                     │
  │  │  resultPath: DISCARD │     │                       │                     │
  │  │                      │     │  fn: {stage}-trunkful-│                     │
  │  │  ┌────────────────┐  │     │  inv-process-item     │                     │
  │  │  │ Per-item       │  │     │                       │                     │
  │  │  │ iterator:      │  │     │  payloadResponseOnly: │                     │
  │  │  │                │  │     │    true                │                     │
  │  │  │ ProcessReser-  │  │     │                       │                     │
  │  │  │ vationItem     │  │     │  (same Lambda as the  │                     │
  │  │  │ (LambdaInvoke) │  │     │   Map iterator, just  │                     │
  │  │  │                │  │     │   called without       │                     │
  │  │  │ fn: {stage}-   │  │     │   isReservation=true)  │                     │
  │  │  │ trunkful-inv-  │  │     └──────────────────────┘                     │
  │  │  │ process-item   │  │                                                   │
  │  │  │                │  │                                                   │
  │  │  │ payloadResp-   │  │                                                   │
  │  │  │ onseOnly: true │  │                                                   │
  │  │  └────────────────┘  │                                                   │
  │  └──────────────────────┘                                                   │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘
```

### ProcessItem Lambda: What It Does

The `process-item` Lambda handles both paths (reservation via Map, and generic
adjustment). Its behavior depends on the `isReservation` flag:

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │  ProcessItem Lambda (inventory-steps/process-item.ts)              │
  │                                                                     │
  │  Input: { sku, quantity, warehouseId?, orderId?, isReservation? }  │
  │                                                                     │
  │  1. Determine adjustment direction:                                 │
  │     isReservation=true  → adjustBy = -abs(quantity) (subtract)     │
  │     isReservation=false → adjustBy = quantity (add as-is)          │
  │                                                                     │
  │  2. Idempotency guard (only when orderId is present):              │
  │     ┌───────────────────────────────────────────────────────┐      │
  │     │  DDB PutItem (conditional)                            │      │
  │     │  Table: idempotency-table                             │      │
  │     │  Key:   pk = "INV#{orderId}#{sku}#{warehouseId}"     │      │
  │     │  TTL:   now + 86400 (24 hours)                       │      │
  │     │  Condition: attribute_not_exists(pk)                  │      │
  │     │                                                       │      │
  │     │  If key already exists (ConditionalCheckFailed):      │      │
  │     │    → return { sku, warehouseId, skipped: true }       │      │
  │     │    → NO inventory adjustment, NO events emitted       │      │
  │     └───────────────────────────────────────────────────────┘      │
  │                                                                     │
  │  3. Atomic inventory adjustment:                                    │
  │     DDB UpdateItem on inventory table                              │
  │       Key:  pk = "SKU#{sku}", sk = "WAREHOUSE#{warehouseId}"      │
  │       UpdateExpression: ADD quantity :qty                           │
  │       ReturnValues: ALL_NEW                                        │
  │                                                                     │
  │  4. Emit InventoryAdjusted event:                                   │
  │     source: trunkful.orders                                        │
  │     { sku, warehouseId, adjustedBy, newQuantity, reason? }         │
  │     InventoryAdjusted → Rule 6: AllEvents→Firehose only           │
  │     (intentionally NOT routed to inventory queue to avoid loops)   │
  │                                                                     │
  │  5. If newQuantity < 10 (LOW_STOCK_THRESHOLD):                     │
  │     Emit InventoryLow event:                                       │
  │     { sku, warehouseId, currentQuantity, threshold: 10 }           │
  │     InventoryLow → Rule 5: NotificationEvents→NotificationQueue   │
  │                  → Rule 6: AllEvents→Firehose                      │
  │                                                                     │
  │  Return: { sku, warehouseId, newQuantity, lowStock: bool }         │
  └─────────────────────────────────────────────────────────────────────┘
```

### Why Idempotency Is Needed

The Map state processes each item in the order via a separate Lambda invocation.
If any single item's Lambda fails, Step Functions retries that specific
iteration. The problem: the DynamoDB `ADD` operation used for inventory
adjustment is NOT idempotent -- calling `ADD quantity -3` twice subtracts 6
instead of 3.

```
  WITHOUT IDEMPOTENCY:

  Map iteration for SKU-001:
    Attempt 1: ADD quantity -3    (stock: 100 → 97)
    Lambda timeout after DDB write but before return
    Step Functions retries...
    Attempt 2: ADD quantity -3    (stock: 97 → 94)  ← WRONG! Double-counted!


  WITH IDEMPOTENCY:

  Map iteration for SKU-001:
    Attempt 1: PutItem INV#ord-123#SKU-001#DEFAULT (condition: not exists) → OK
               ADD quantity -3    (stock: 100 → 97)
               Lambda timeout after DDB write but before return
               Step Functions retries...
    Attempt 2: PutItem INV#ord-123#SKU-001#DEFAULT (condition: not exists)
               → ConditionalCheckFailedException!
               → return { skipped: true }
               Stock stays at 97. Correct!
```

The idempotency key format `INV#{orderId}#{sku}#{warehouseId}` is scoped per
order, per SKU, per warehouse, so the same order cannot adjust the same item
twice. The 24-hour TTL auto-expires old keys.

---

## 4. Trigger Lambdas

### Why Thin Triggers Exist

SQS cannot directly invoke Step Functions. The integration path is:

```
  EventBridge Rule → SQS Queue → Lambda (trigger) → StartExecution → State Machine
```

The trigger Lambdas are intentionally "thin" -- they contain no business logic.
Their only job is to parse the SQS message body and call `StartExecution`. This
keeps the SQS retry/DLQ semantics working correctly: if the trigger Lambda
crashes before calling StartExecution, SQS retries it (up to `maxReceiveCount`
times, then routes to DLQ). If the state machine itself fails, that is handled
internally by Step Functions Catch/compensation -- the SQS message is deleted
because the trigger Lambda succeeded.

### Order Saga Trigger

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  order-steps/trigger.ts                                             │
  │  fn: {stage}-trunkful-order-saga-trigger                            │
  │                                                                      │
  │  SQS event source: order-queue (batchSize: 1)                       │
  │  Queue fed by: Rule 1: OrderCreated→OrderQueue                      │
  │                                                                      │
  │  For each SQS record:                                                │
  │    1. Parse record.body as JSON (EventBridge envelope)               │
  │    2. Extract order = envelope.detail ?? envelope                    │
  │       (fallback handles both envelope and raw order)                │
  │    3. StartExecution:                                                │
  │       stateMachineArn: {stage}-trunkful-order-saga                  │
  │       name: "order-{orderId}-{Date.now()}"                          │
  │       input: JSON.stringify(order)                                   │
  │                                                                      │
  │  The execution name "order-{orderId}-{timestamp}" prevents          │
  │  duplicate executions for the same order within the same             │
  │  millisecond (Standard state machines reject duplicate names).       │
  └──────────────────────────────────────────────────────────────────────┘
```

### Inventory Workflow Trigger

```
  ┌──────────────────────────────────────────────────────────────────────┐
  │  inventory-steps/trigger.ts                                         │
  │  fn: {stage}-trunkful-inventory-workflow-trigger                     │
  │                                                                      │
  │  SQS event source: inventory-queue (batchSize: 1)                   │
  │  Queue fed by: Rule 2: InventoryEvents→InventoryQueue               │
  │    (matches: OrderCreated, InventoryReceived, ReturnInitiated)       │
  │                                                                      │
  │  For each SQS record:                                                │
  │    1. Parse record.body as JSON (EventBridge envelope)               │
  │    2. Pass the FULL envelope (not just .detail)                      │
  │       because the state machine needs "detail-type" to branch       │
  │    3. StartExecution:                                                │
  │       stateMachineArn: {stage}-trunkful-inventory-workflow           │
  │       name: "inv-{Date.now()}-{record.messageId}"                   │
  │       input: JSON.stringify(envelope)                                │
  │                                                                      │
  │  Key difference from order trigger: passes the FULL EventBridge      │
  │  envelope (including "detail-type") so NormalizeInput can extract    │
  │  it and the Choice state can branch on event type.                  │
  └──────────────────────────────────────────────────────────────────────┘
```

### Trigger Comparison

```
  ┌─────────────────────────┬────────────────────────┬──────────────────────────┐
  │                         │  Order Saga Trigger     │  Inventory Trigger       │
  ├─────────────────────────┼────────────────────────┼──────────────────────────┤
  │  Input to SM            │  order object only      │  full EventBridge        │
  │                         │  (envelope.detail)      │  envelope                │
  ├─────────────────────────┼────────────────────────┼──────────────────────────┤
  │  Execution name         │  order-{orderId}-       │  inv-{timestamp}-        │
  │                         │  {timestamp}            │  {messageId}             │
  ├─────────────────────────┼────────────────────────┼──────────────────────────┤
  │  Why this shape?        │  SM only processes one  │  SM needs detail-type    │
  │                         │  order type, no need    │  to choose between       │
  │                         │  for event-type routing │  OrderCreated (Map) and  │
  │                         │                        │  generic (direct invoke) │
  └─────────────────────────┴────────────────────────┴──────────────────────────┘
```

---

## 5. Data Flow Through States

### How payloadResponseOnly Works

Every `LambdaInvoke` task in both state machines sets `payloadResponseOnly: true`.
Without this flag, Step Functions wraps the Lambda response in a metadata envelope:

```
  payloadResponseOnly: false (default):
    { "StatusCode": 200, "Payload": { ...actual data... } }

  payloadResponseOnly: true:
    { ...actual data... }
```

By setting it to `true`, the Lambda's return value is used directly as the
state's result, which makes `resultPath` configuration predictable and avoids
having to reference `$.Payload` everywhere.

### Data Flow Through the Order Saga

```
  INITIAL STATE INPUT (from trigger):
  {
    "orderId": "abc-123",
    "customerId": "cust-456",
    "items": [{ "sku": "SKU-001", "quantity": 2 }],
    "totalAmount": 49.99
  }

       │
       v
  ValidateOrder
    Input:  { orderId, customerId, items, totalAmount }
    Lambda returns: same object (pass-through)
    resultPath: DISCARD → original input unchanged
    Output: { orderId, customerId, items, totalAmount }    ← same as input
       │
       v
  ReserveInventory
    Input:  { orderId, customerId, items, totalAmount }    ← same
    Lambda returns: same object (pass-through)
    resultPath: DISCARD → original input unchanged
    Output: { orderId, customerId, items, totalAmount }    ← same
       │
       v
  ProcessPayment
    Input:  { orderId, customerId, items, totalAmount }    ← same
    Lambda returns: { orderId, customerId, items, totalAmount, transactionId }
    resultPath: '$' → Lambda output REPLACES entire state
    Output: { orderId, customerId, items, totalAmount, transactionId }  ← ENRICHED
       │
       v
  ConfirmOrder
    Input:  { orderId, customerId, items, totalAmount, transactionId }  ← has txnId
    Lambda reads transactionId, writes it to DDB, includes it in event
    resultPath: DISCARD
    Output: (execution ends)
```

### Error Data Flow via addCatch

When a step fails and `addCatch` fires, Step Functions merges error information
into the state data at the configured `resultPath`:

```
  ProcessPayment throws Error('Payment declined')
       │
       │  addCatch fires with resultPath: '$.error'
       │
       v
  State data becomes:
  {
    "orderId": "abc-123",
    "customerId": "cust-456",
    "items": [...],
    "totalAmount": 49.99,
    "error": {                       ← injected by addCatch
      "Error": "Error",
      "Cause": "Payment declined"
    }
  }
       │
       v
  CompensateRelease receives this ^ and reads event.orderId, event.items
       │
       v
  MarkFailedAfterComp receives this ^ and reads:
    event.error.Cause → "Payment declined" (used as failure reason)
    event.orderId     → "abc-123"
```

---

## 6. Retry and Error Handling

### Per-State Retry Configuration

All four main saga steps share identical retry config:

```
  step.addRetry({
    errors: ['States.TaskFailed'],     // catches any Lambda error
    maxAttempts: 2,                     // retry up to 2 times (3 total attempts)
    backoffRate: 2,                     // exponential backoff multiplier
    interval: Duration.seconds(1),      // initial wait: 1 second
  });

  Timeline of a failing step:

  Attempt 1: invoke Lambda → fails
             wait 1 second
  Attempt 2: invoke Lambda → fails
             wait 2 seconds (1s * backoffRate 2)
  Attempt 3: invoke Lambda → fails
             ALL RETRIES EXHAUSTED → Catch fires
```

### How Retry and Catch Interact

```
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                                                                          │
  │  Step Functions evaluates Retry FIRST, Catch SECOND.                     │
  │                                                                          │
  │  Lambda invoked → ERROR                                                  │
  │       │                                                                  │
  │       v                                                                  │
  │  Does the error match a Retry rule?                                      │
  │       │                                                                  │
  │   YES │                              NO                                  │
  │       v                               │                                  │
  │  Are retries exhausted?                │                                 │
  │       │                                │                                 │
  │   NO  │         YES                    │                                 │
  │       v          │                     │                                 │
  │  Wait & retry    │                     │                                 │
  │  (back to        v                     v                                 │
  │   Lambda)   Does the error match a Catch rule?                           │
  │                   │                                                      │
  │              YES  │                NO                                     │
  │                   v                 │                                     │
  │             Transition to            v                                    │
  │             Catch target       Execution fails                           │
  │             (compensation)     with unhandled error                      │
  │                                                                          │
  └──────────────────────────────────────────────────────────────────────────┘
```

Since the retry config uses `States.TaskFailed` (which catches all Lambda
errors) and the catch config uses the default (all errors), the flow is:

1. Lambda fails
2. Retry up to 2 more times with backoff
3. If still failing after 3 total attempts, Catch fires
4. Catch transitions to the appropriate compensation chain

### Compensation States Have No Retry

The compensation and failure-marking states (`MarkFailedEarly`,
`CompensateRelease`, `MarkFailedAfterComp`) do NOT have `addRetry()` configured.
If compensation itself fails, the state machine execution fails with an
unhandled error. This is intentional -- if we cannot even mark an order as
failed, something is fundamentally wrong and human intervention is needed.
The SQS trigger Lambda will have already returned successfully (because it
started the execution), so the SQS message is already deleted. The failed
Step Functions execution will be visible in the console and can trigger a
CloudWatch alarm.

---

## 7. CDK Implementation Patterns

### CDK Constructs Used

All Step Functions infrastructure is defined in
`lib/constructs/processing.ts` inside the `ProcessingConstruct`.

```
  ┌──────────────────────────┬──────────────────────────────────────────────┐
  │  CDK Construct           │  Usage                                       │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  sfn.StateMachine        │  Two instances: order-saga and inventory-    │
  │                          │  workflow. Both use DefinitionBody.          │
  │                          │  fromChainable() and 5-min timeout.         │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  tasks.LambdaInvoke      │  Wraps each step Lambda. Every instance     │
  │                          │  sets payloadResponseOnly: true.            │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  sfn.Pass                │  NormalizeInput: converts detail-type →     │
  │                          │  detailType using bracket notation.         │
  │                          │  ExtractGenericDetail: extracts $.detail    │
  │                          │  for the generic inventory path.            │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  sfn.Choice              │  InventoryEventType: branches on            │
  │                          │  $.detailType == "OrderCreated".            │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  sfn.Map                 │  MapOrderItems: iterates $.detail.items     │
  │                          │  with itemSelector that enriches each item  │
  │                          │  with orderId and isReservation=true.       │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  .addRetry()             │  Applied to 4 main saga steps. Config:     │
  │                          │  States.TaskFailed, maxAttempts: 2,        │
  │                          │  backoffRate: 2, interval: 1s.             │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  .addCatch()             │  ValidateOrder + ReserveInventory catch     │
  │                          │  to MarkFailedEarly.                        │
  │                          │  ProcessPayment + ConfirmOrder catch to     │
  │                          │  CompensateRelease.                         │
  │                          │  All use resultPath: '$.error'.            │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  sfn.JsonPath.DISCARD    │  Used as resultPath on most steps to       │
  │                          │  preserve the original state input.        │
  ├──────────────────────────┼──────────────────────────────────────────────┤
  │  .grantStartExecution()  │  Gives each trigger Lambda permission to   │
  │                          │  start its respective state machine.       │
  └──────────────────────────┴──────────────────────────────────────────────┘
```

### Lambda Creation Pattern

All step Lambdas are created with a shared `createStepFn` helper:

```
  createStepFn(constructId, entryPath, envVars, timeout?)

  Defaults:
    runtime:    NODEJS_22_X
    handler:    'handler'
    memorySize: 128 MB
    timeout:    10 seconds (15s for ProcessPayment)
    tracing:    X-Ray ACTIVE
    naming:     {stage}-trunkful-{kebab-case-constructId}
```

### Permission Grants

```
  ┌─────────────────────────┬──────────────────────┬──────────────────────────┐
  │  Lambda                 │  DynamoDB             │  EventBridge             │
  ├─────────────────────────┼──────────────────────┼──────────────────────────┤
  │  validateFn             │  ordersTable: RW      │  eventBus: PutEvents    │
  │  reserveInventoryFn     │  ordersTable: RW      │  eventBus: PutEvents    │
  │  processPaymentFn       │  ordersTable: RW      │  (none -- no events)    │
  │                         │  + paymentSecret: Read│                          │
  │  confirmOrderFn         │  ordersTable: RW      │  eventBus: PutEvents    │
  │  releaseInventoryFn     │  (none)               │  eventBus: PutEvents    │
  │  markFailedFn           │  ordersTable: RW      │  eventBus: PutEvents    │
  │  processItemFn          │  inventoryTable: RW   │  eventBus: PutEvents    │
  │                         │  idempotencyTable: RW │                          │
  │  orderSagaTriggerFn     │  (none)               │  (none)                 │
  │                         │  + SM: StartExecution │                          │
  │  invWorkflowTriggerFn   │  (none)               │  (none)                 │
  │                         │  + SM: StartExecution │                          │
  └─────────────────────────┴──────────────────────┴──────────────────────────┘
```

### State Machine Chain Construction

The CDK uses a fluent `.next()` API to build the state machine chains:

```typescript
  // Order Saga: linear chain with Catch branches
  const orderSagaChain = validateOrder
    .next(reserveInventory)
    .next(processPayment)
    .next(confirmOrder);

  // Inventory Workflow: Pass → Choice → (Map | Pass → Lambda)
  const inventoryDefinition = normalizeInput.next(eventTypeChoice);
  // where eventTypeChoice is a Choice that wires to mapOrderItems or
  // extractGenericDetail.next(processGenericItem)
```

The compensation chain is built separately and wired via `addCatch()`:

```typescript
  compensateRelease.next(markFailedAfterComp);  // comp chain

  processPayment.addCatch(compensateRelease, { resultPath: '$.error' });
  confirmOrder.addCatch(compensateRelease, { resultPath: '$.error' });
```

This means `MarkFailedEarly` and the `CompensateRelease → MarkFailedAfterComp`
chain are not part of the main `.next()` chain -- they are only reachable via
error paths, which is exactly what makes this a saga pattern.
