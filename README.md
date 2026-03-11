# TrunkfulCDK

Event-driven inventory and order processing system for a mid-size retailer.
AWS CDK (TypeScript). 5 ingestion channels, Step Functions sagas,
EventBridge routing, SQS buffering + DLQ resilience.

---

## Code

```
TrunkfulCDK/
│
├── bin/
│   └── trunkful.ts                              CDK app entry: pipeline or local-dev
│
├── lib/
│   ├── pipeline-stack.ts                        CDK Pipelines: Source → Synth → Alpha → Prod
│   ├── trunkful-stage.ts                        cdk.Stage wrapper (Alpha / Prod)
│   ├── trunkful-stack.ts                        Main stack: composes all constructs,
│   │                                            wires 6 EventBridge rules
│   ├── stage-config.ts                          Stage config (concurrency, removal policy)
│   ├── constructs/
│   │   ├── security.ts                          KMS CMK (PII) + Secrets Manager (payment key)
│   │   ├── data-layer.ts                        DynamoDB: Orders, Inventory, Idempotency
│   │   ├── event-bus.ts                         Custom EventBridge bus + 90-day archive
│   │   ├── queues.ts                            5 SQS queues + 5 DLQs (maxReceiveCount: 3)
│   │   ├── ingestion-api.ts                     API Gateway + Cognito + WAF + DDB direct read
│   │   ├── ingestion-iot.ts                     IoT Core topic rule → POS Lambda
│   │   ├── ingestion-s3.ts                      S3 upload bucket → admin ingest Lambda
│   │   ├── ingestion-warehouse.ts               IAM role for direct PutEvents
│   │   ├── processing.ts                        Step Functions state machines + step Lambdas + SQS event sources
│   │   ├── analytics.ts                         Firehose → S3 → Glue → Athena
│   │   └── monitoring.ts                        CloudWatch dashboard + 6 alarms
│   └── shared/
│       └── lambda-defaults.ts                   Shared Lambda config (Node 22, X-Ray, 256MB)
│
├── lambda/
│   ├── shared/
│   │   ├── types.ts                             OrderStatus, EventType, Channel enums
│   │   ├── idempotency.ts                       DDB conditional PutItem (24h TTL)
│   │   ├── event-emitter.ts                     EventBridge PutEvents helper
│   │   └── logger.ts                            Structured JSON logger
│   ├── order-intake/index.ts                    Web/Mobile → API GW → DDB + emit OrderCreated
│   ├── pos-intake/index.ts                      POS terminal → IoT Core → DDB + emit
│   ├── webhook-intake/index.ts                  Supplier webhook → header auth → DDB + emit
│   ├── admin-ingest/index.ts                    S3 JSON upload → batch process → DDB + emit
│   ├── order-service/
│   │   ├── circuit-breaker.ts                   DDB-backed circuit breaker (5 failures → OPEN)
│   │   └── payment-client.ts                    Payment provider stub
│   ├── order-steps/
│   │   ├── validate.ts                          Step 1: validate order fields, DDB → VALIDATING
│   │   ├── reserve-inventory.ts                 Step 2: DDB → RESERVED, emit OrderReserved
│   │   ├── process-payment.ts                   Step 3: circuit breaker + payment call
│   │   ├── confirm-order.ts                     Step 4: DDB → CONFIRMED, emit OrderConfirmed
│   │   ├── release-inventory.ts                 Compensation: emit InventoryReleaseRequested
│   │   ├── mark-failed.ts                       Terminal: DDB → FAILED, emit OrderFailed
│   │   ├── trigger.ts                            SQS → StartExecution trigger
│   │   └── shared.ts                            DDB/EventBridge helpers
│   ├── inventory-steps/
│   │   ├── process-item.ts                      Single item adjustment + idempotency guard
│   │   └── trigger.ts                            SQS → StartExecution trigger
│   ├── billing/index.ts                         Billing stub
│   ├── fulfillment/index.ts                     Fulfillment stub
│   └── notification/index.ts                    SES/SNS + KMS PII decrypt
│
├── test/
│   └── trunkful-stack.test.ts                   18 assertions + pipeline synth test
│
└── docs/
    ├── v1-design.md                             Original design overview
    ├── event-routing.md                         Event → rule → queue diagrams
    ├── happy-path.md                            Order confirmed walkthrough
    ├── failure-path.md                          3 failure scenarios + resilience layers
    ├── order-service-flow.md                    Step Functions saga + SQS DLQ detail
    ├── inventory-flow.md                        Step Functions Map state + idempotency + feedback loop fix
    └── cdk-getting-started.md                   CDK CLI commands reference
```

---

## Architecture

```
  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
  │   Web    │ │  Mobile  │ │   POS    │ │Warehouse │ │ Supplier │
  │Storefront│ │   App    │ │Terminals │ │ Scanners │ │ Webhooks │
  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘
       │            │            │            │            │
       v            v            v            v            v
  ┌─────────────┐  ┌──────┐  ┌──────┐  ┌──────────┐  ┌─────────┐
  │ API Gateway │  │ API  │  │ IoT  │  │  Direct  │  │   API   │
  │ + Cognito   │  │ GW   │  │ Core │  │ PutEvents│  │   GW    │
  │ + WAF       │  │      │  │      │  │ (IAM)    │  │         │
  └──────┬──────┘  └──┬───┘  └──┬───┘  └────┬─────┘  └────┬────┘
         │            │         │            │             │
         v            v         v            │             v
  ┌────────────┐ ┌────────┐ ┌────────┐       │      ┌──────────┐
  │  order-    │ │ order- │ │ pos-   │       │      │ webhook- │
  │  intake    │ │ intake │ │ intake │       │      │ intake   │
  │  Lambda    │ │ Lambda │ │ Lambda │       │      │ Lambda   │
  └──────┬─────┘ └───┬────┘ └───┬────┘       │      └────┬─────┘
         │           │          │            │           │
         └─────┬─────┴──────┬──┴────────────┴───────────┘
               │            │
               │ DDB Put    │ PutEvents
               │ (PENDING)  │ (OrderCreated)
               v            v
  ┌──────────────┐  ┌─────────────────────────────────────────────┐
  │   DynamoDB   │  │            TrunkfulEventBus                  │
  │   Orders     │  │         (custom EventBridge bus)             │
  │   table      │  │                                              │
  │              │  │  6 rules evaluate in parallel:               │
  │  PENDING     │  │  pattern-match on detail-type                │
  └──────────────┘  └──────────────────┬──────────────────────────┘
                                       │
         ┌─────────┬──────────┬────────┼────────┬──────────┐
         │         │          │        │        │          │
         │ Order   │ Order    │ Order  │ Order  │ Order    │ ALL
         │ Created │ Created  │Confirm-│Confirm-│Confirmed │events
         │         │ +Inv.Rcvd│ ed     │ ed     │+Failed   │
         │         │ +Return  │        │        │+Inv.Low  │
         v         v          v        v        v          v
  ┌──────────┐ ┌────────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌────────┐
  │  order   │ │inven-  │ │bill- │ │ful-  │ │noti- │ │Firehose│
  │  queue   │ │tory    │ │ing   │ │fill- │ │fica- │ │→ S3    │
  │          │ │queue   │ │queue │ │ment  │ │tion  │ │→ Glue  │
  │ batch:1  │ │batch:1 │ │      │ │queue │ │queue │ │→ Athena│
  └────┬─────┘ └───┬────┘ └──┬───┘ └──┬───┘ └──┬───┘ └────────┘
       │ SQS poll  │SQS poll │        │        │
       v           v         v        v        v
  ┌─────────┐ ┌────────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │ order   │ │inven-  │ │bill- │ │ful-  │ │noti- │
  │ saga    │ │tory    │ │ing   │ │fill- │ │fica- │
  │ trigger │ │workflow│ │      │ │ment  │ │tion  │
  │    │    │ │trigger │ │      │ │      │ │      │
  └────┼────┘ └───┼────┘ └──────┘ └──────┘ └──────┘
       │Start     │Start
       │Execution │Execution
       v          v
  ┌─────────┐ ┌────────┐
  │ Order   │ │Inven-  │
  │ Saga    │ │tory    │
  │ State   │ │Workflow│
  │ Machine │ │  SM    │
  └─────────┘ └────────┘
```

---

## Event Routing

Every event enters the bus with `source: trunkful.orders`. Six rules
evaluate in parallel. Each rule pattern-matches on `detail-type`.

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

`InventoryAdjusted` is intentionally **not** routed to the inventory queue.
It is a notification ("this happened"), not a command ("do this"). Routing it
back would create an infinite feedback loop with double-counted stock.

---

## Happy Path

```
  Customer places order via POST /orders (Cognito auth)

  ┌──────────┐  POST /orders  ┌──────────────┐                ┌─────────────────┐
  │  Web /   │───────────────>│ order-intake  │──PutEvents───>│ TrunkfulEventBus │
  │  Mobile  │  (Cognito auth)│   Lambda      │ (OrderCreated)│                  │
  └──────────┘                └──────┬───────┘               └────────┬──────────┘
                                     │                                │
                                     │ DDB PutItem                    │
                                     v (status: PENDING)              │
                              ┌─────────────┐                        │
                              │  DynamoDB   │     OrderCreated event  │
                              │  Orders     │     OrderCreated matches│
                              │             │     Rule 1, 2, 6       │
                              │  PENDING    │                        │
                              └─────────────┘                        │
                                             ┌───────────────────────┤
                                             │ Rule 1               │ Rule 2
                                             │ (OrderCreated        │ (InvEvents
                                             │  →OrderQ)            │  →InvQ)
                                             v                      v
                                      ┌────────────┐       ┌─────────────┐
                                      │order-queue │       │inventory-   │
                                      │   (SQS)    │       │queue (SQS)  │
                                      └─────┬──────┘       └──────┬──────┘
                                            │ SQS poll            │ SQS poll
                                            v                     v
                                     ┌────────────┐       ┌────────────┐
                                     │ order-saga │       │ inventory- │
                                     │ trigger λ  │       │ workflow   │
                                     └─────┬──────┘       │ trigger λ  │
                                           │              └──────┬─────┘
                                           │StartExecution       │StartExecution
                                           v                     v
  ┌──────────────────────────────────────────────┐  ┌──────────────────────────┐
  │  ORDER SAGA (Step Functions)                  │  │  INVENTORY WORKFLOW (SF) │
  │                                               │  │                          │
  │  ValidateOrder (Lambda)                       │  │  Choice: OrderCreated    │
  │    DDB → VALIDATING, emit OrderValidated      │  │    → Map (per item):    │
  │                                               │  │       ProcessItem (λ)   │
  │  ReserveInventory (Lambda)                    │  │         DDB ADD qty: -N  │
  │    DDB → RESERVED, emit OrderReserved         │  │         emit Inventory-  │
  │                                               │  │           Adjusted       │
  │  ProcessPayment (Lambda)                      │  │                          │
  │    circuit breaker → processPayment()         │  │       (if qty < 10)      │
  │                                               │  │         emit InventoryLow│
  │  ConfirmOrder (Lambda)                        │  └──────────────────────────┘
  │    DDB → CONFIRMED, emit OrderConfirmed ─────────────────────────────────┐
  │                                               │                          │
  └───────────────────────────────────────────────┘                          │
                                                                             │
    emit OrderConfirmed ──> EventBridge ──> matches Rule 3, 4, 5, 6       │
               ┌───────────────────┬───────────────────┐                  │
               │ Rule 3            │ Rule 4            │ Rule 5           │Rule 6
               │ (Confirmed       │ (Confirmed        │ (Notification    │(All→
               │  →BillingQ)       │  →FulfillQ)       │  Events)         │Firehose)
               v                   v                   v                  v
        ┌────────────┐     ┌─────────────┐     ┌────────────┐     ┌───────────┐
        │billing-    │     │fulfillment- │     │notification│     │ Firehose  │
        │queue (SQS) │     │queue (SQS)  │     │-queue (SQS)│     │ → S3      │
        └─────┬──────┘     └──────┬──────┘     └─────┬──────┘     │ → Athena  │
              │ SQS poll          │ SQS poll          │ SQS poll   └───────────┘
              v                   v                   v
        ┌────────────┐     ┌─────────────┐     ┌────────────┐
        │  BILLING   │     │ FULFILLMENT │     │NOTIFICATION│
        │  generate  │     │  initiate   │     │  send email│
        │  invoice   │     │  shipping   │     │  (SES)     │
        └────────────┘     └─────────────┘     └────────────┘


  Order status: PENDING ──> VALIDATING ──> RESERVED ──> CONFIRMED
```

---

## Failure Path: Payment Declined

Step Functions Catch fires, compensation states run inline.
The state machine completes normally. SQS deletes the message.

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  ORDER SAGA (Step Functions)                                     │
  │                                                                  │
  │  ValidateOrder               ── OK                               │
  │  ReserveInventory            ── OK                               │
  │  ProcessPayment              ── DECLINED ── THROWS               │
  │         │                                                        │
  │         v  Step Functions Catch fires                             │
  │                                                                  │
  │  ┌─────────────────────────────────────────────┐                 │
  │  │  COMPENSATION STATES                         │                 │
  │  │                                              │                 │
  │  │  CompensateRelease (Lambda)                  │                 │
  │  │    └─ emit InventoryReleaseRequested         │                 │
  │  │                                              │                 │
  │  │  MarkFailedAfterComp (Lambda)                │                 │
  │  │    ├─ DDB → FAILED                           │                 │
  │  │    └─ emit OrderFailed ─────────────────────────────────────┐ │
  │  └─────────────────────────────────────────────┘               │ │
  │                                                                 │ │
  │  Execution completes ← compensation ran, no SQS retry needed    │ │
  └─────────────────────────────────────────────────────────────────┘ │
                                                                      │
                  OrderFailed → Rule 5 + 6                            │
                  ┌────────────────────┐                              │
                  v                    v                              │
           ┌────────────┐      ┌───────────┐                         │
           │NOTIFICATION│      │ Firehose  │                         │
           │"Sorry..."  │      │ → S3      │                         │
           └────────────┘      └───────────┘

  Order status: PENDING ──> VALIDATING ──> RESERVED ──> FAILED
                                                          │
                                              inventory released,
                                              customer notified
```

---

## Failure Path: Trigger Lambda Crash → SQS Retry → DLQ

If the trigger Lambda crashes before calling `StartExecution`, SQS
retries 3 times. If the state machine itself fails, Step Functions
Catch states handle compensation inline — no SQS retry needed for
business failures.

```
  SQS: order-queue                   │
  ┌──────────────────────────────────┘
  │
  │  ATTEMPT 1 (receiveCount: 1)
  │  ┌──────────────────────────────────────────────────┐
  │  │  Starter Lambda → StartExecution                  │
  │  │  State Machine runs:                              │
  │  │    ValidateOrder    → OK                          │
  │  │    ReserveInventory → OK                          │
  │  │    ProcessPayment   → THROWS (service down)       │
  │  │    Catch → CompensateRelease → MarkFailed         │
  │  │  SM completes with compensation. No retry needed. │
  │  └──────────────────────────────────────────────────┘
  │
  │  If trigger itself crashes (rare):
  │  ┌──────────────────────────────────────────────────┐
  │  │  ATTEMPT 2 → trigger crashes again                │
  │  │  ATTEMPT 3 → trigger crashes again                │
  │  │                                                   │
  │  │  receiveCount (3) >= maxReceiveCount (3)          │
  │  └──────────────────────────────────────────────────┘
  │
  v
  ┌──────────────────────────────────────────────────┐
  │  SQS: order-dlq (Dead Letter Queue)               │
  │                                                    │
  │  Message parked. 14-day retention.                 │
  │  CloudWatch alarm fires → engineer investigates.   │
  │  After fix: redrive from DLQ.                      │
  └──────────────────────────────────────────────────┘
```

---

## Inventory Service

Three event types route to the inventory queue as commands:

```
  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐
  │  Intake Lambdas    │  │  Warehouse         │  │  Order Service     │
  │                    │  │  Scanners          │  │  (compensation)    │
  │  emit:             │  │  emit:             │  │  emit:             │
  │  OrderCreated      │  │  InventoryReceived │  │  InventoryRelease- │
  │                    │  │  ReturnInitiated   │  │  Requested         │
  └────────┬───────────┘  └────────┬───────────┘  └────────┬───────────┘
           │                       │                       │
           └───────────┬───────────┴───────────────────────┘
                       │
                       v
              ┌─────────────────┐
              │ inventory-queue │
              │ (batchSize: 1)  │
              └────────┬────────┘
                       │
                       v
              ┌─────────────────────────────────────────────┐
              │  trigger Lambda → StartExecution              │
              └────────────────────┬──────────────────────────┘
                                   │
                                   v
              ┌─────────────────────────────────────────────┐
              │  INVENTORY WORKFLOW (Step Functions)          │
              │                                               │
              │  NormalizeInput (Pass) → Choice:              │
              │                                               │
              │  OrderCreated ──> Map (per item):             │
              │    ProcessItem (Lambda):                      │
              │      idempotency check                       │
              │      DDB: ADD quantity = -(ordered qty)      │
              │      emit InventoryAdjusted (→ analytics)    │
              │      if qty < 10:                             │
              │        emit InventoryLow (→ notification)    │
              │                                               │
              │  InventoryReceived / ReturnInitiated ──>      │
              │    ProcessItem (Lambda):                      │
              │      DDB: ADD quantity = +N                   │
              │      emit InventoryAdjusted (→ analytics)    │
              │      if qty < 10:                             │
              │        emit InventoryLow (→ notification)    │
              └─────────────────────────────────────────────┘
```

DynamoDB layout — one row per SKU per warehouse, atomic ADD operations:

```
  ┌──────────────────┬─────────────────────┬────────┐
  │ pk               │ sk                  │quantity │
  ├──────────────────┼─────────────────────┼────────┤
  │ SKU#SKU-100      │ WAREHOUSE#DEFAULT   │   48   │
  │ SKU#SKU-100      │ WAREHOUSE#WH-EAST   │  200   │
  │ SKU#SKU-100      │ WAREHOUSE#WH-WEST   │  175   │
  │ SKU#SKU-200      │ WAREHOUSE#DEFAULT   │    7   │ ← LOW
  │ SKU#SKU-200      │ WAREHOUSE#WH-EAST   │  507   │
  │ SKU#SKU-300      │ WAREHOUSE#DEFAULT   │  120   │
  └──────────────────┴─────────────────────┴────────┘
```

---

## Three Layers of Resilience

```
  ┌─────────────────────────────────────────────────────────────────┐
  │                                                                 │
  │   Layer 1: APPLICATION LOGIC                                    │
  │   Step Functions Catch + compensation states                    │
  │   ├─ Payment declined? → CompensateRelease → MarkFailed         │
  │   ├─ Validation fails? → MarkFailedEarly                        │
  │   └─ Handles EXPECTED business failures with visible state flow │
  │                                                                 │
  │   ┌─────────────────────────────────────────────────────────┐   │
  │   │                                                         │   │
  │   │   Layer 2: STEP FUNCTIONS ORCHESTRATION                 │   │
  │   │   Per-state retry + execution history                   │   │
  │   │   ├─ Transient failure? → retry 2x with backoff         │   │
  │   │   ├─ All retries fail? → Catch → compensation states    │   │
  │   │   └─ Handles INFRASTRUCTURE failures within execution   │   │
  │   │                                                         │   │
  │   │   ┌─────────────────────────────────────────────────┐   │   │
  │   │   │                                                 │   │   │
  │   │   │   Layer 3: SQS RETRY + DLQ                      │   │   │
  │   │   │   maxReceiveCount: 3 + dead letter queue         │   │   │
  │   │   │   ├─ Trigger crashes? → SQS retries 3x             │   │   │
  │   │   │   ├─ All retries fail? → message goes to DLQ    │   │   │
  │   │   │   ├─ CloudWatch alarm fires                      │   │   │
  │   │   │   └─ Handles PERSISTENT failures, needs human    │   │   │
  │   │   │                                                 │   │   │
  │   │   └─────────────────────────────────────────────────┘   │   │
  │   │                                                         │   │
  │   └─────────────────────────────────────────────────────────┘   │
  │                                                                 │
  └─────────────────────────────────────────────────────────────────┘
```

---

## Circuit Breaker

Protects the system when an external service (e.g. payment provider) is down.
Stored in DynamoDB with the key pattern `CIRCUIT#{serviceName}`.

```
  Order #1  payment call → timeout → recordFailure()  failureCount: 1
  Order #2  payment call → timeout → recordFailure()  failureCount: 2
  Order #3  payment call → timeout → recordFailure()  failureCount: 3
  Order #4  payment call → timeout → recordFailure()  failureCount: 4
  Order #5  payment call → timeout → recordFailure()  failureCount: 5
                                                          │
                                              CIRCUIT OPENS (5 failures
                                              within 60 seconds)
                                                          │
                                                          v
  Order #6  checkCircuit() → OPEN → immediate throw, no call made
  Order #7  checkCircuit() → OPEN → immediate throw, no call made
  ...       (all orders fail-fast with compensation)

            ── 30 seconds pass ──

  Order #N  checkCircuit() → HALF_OPEN → allows one test call
            payment call → success → recordSuccess() → CIRCUIT CLOSES
```

---

## CI/CD Pipeline

CDK Pipelines (self-mutating). Source → Synth → Alpha → Manual Approval → Prod.

```
  ┌────────────────────────────────────────────────────────────────────┐
  │                     TrunkfulPipeline                                │
  │                     (CodePipeline, self-mutating)                   │
  │                                                                    │
  │  ┌──────────┐    ┌───────────────────────┐    ┌──────────────┐     │
  │  │  Source   │    │  Synth (CodeBuild)     │    │  UpdatePipeline│   │
  │  │  GitHub   │───>│  npm ci                │───>│  (self-mutate) │   │
  │  │  main     │    │  npx cdk synth         │    │                │   │
  │  └──────────┘    └───────────────────────┘    └──────┬───────┘     │
  │                                                       │             │
  │                    ┌──────────────────────────────────┘             │
  │                    │                                                │
  │                    v                                                │
  │  ┌─────────────────────────────────────────────────┐               │
  │  │  ALPHA STAGE (auto-deploy)                        │               │
  │  │                                                   │               │
  │  │  TrunkfulStack with ALPHA_CONFIG:                 │               │
  │  │    stageName: "alpha"                             │               │
  │  │    removalPolicy: DESTROY                         │               │
  │  │    reservedConcurrency: low (5/5/2/2/2)           │               │
  │  │                                                   │               │
  │  │  Resources prefixed: alpha-order-queue,           │               │
  │  │  alpha-TrunkfulEventBus, alpha-trunkful-*        │               │
  │  └───────────────────────┬─────────────────────────┘               │
  │                          │                                          │
  │                          v                                          │
  │  ┌─────────────────────────────────────────────────┐               │
  │  │  MANUAL APPROVAL                                  │               │
  │  │  "Review Alpha and approve promotion to Prod"     │               │
  │  └───────────────────────┬─────────────────────────┘               │
  │                          │                                          │
  │                          v                                          │
  │  ┌─────────────────────────────────────────────────┐               │
  │  │  PROD STAGE                                       │               │
  │  │                                                   │               │
  │  │  TrunkfulStack with PROD_CONFIG:                  │               │
  │  │    stageName: "prod"                              │               │
  │  │    removalPolicy: RETAIN                          │               │
  │  │    reservedConcurrency: high (50/50/25/25/25)     │               │
  │  │                                                   │               │
  │  │  Resources prefixed: prod-order-queue,            │               │
  │  │  prod-TrunkfulEventBus, prod-trunkful-*          │               │
  │  └─────────────────────────────────────────────────┘               │
  │                                                                    │
  └────────────────────────────────────────────────────────────────────┘
```

Stage isolation — every physical resource name is prefixed with `alpha-` or `prod-`
so both stages can deploy to the same AWS account without collisions.

---

## Quick Start

```bash
npm install                         # Install dependencies
npm test                            # 18 tests (synth + resource assertions)
npx cdk synth                       # Generate CloudFormation (pipeline mode)
LOCAL_DEV=true npx cdk synth        # Generate standalone stack (no pipeline)
LOCAL_DEV=true npx cdk deploy       # Deploy standalone to bootstrapped account
```
