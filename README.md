# TrunkfulCDK

Event-driven inventory and order processing system for a mid-size retailer.
AWS CDK (TypeScript). 5 ingestion channels, durable Lambda sagas,
EventBridge routing, SQS + DLQ resilience.

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
│   │   ├── processing.ts                        5 processing Lambdas + SQS event sources
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
│   │   ├── index.ts                             Durable saga: validate → reserve → pay → confirm
│   │   ├── circuit-breaker.ts                   DDB-backed circuit breaker (5 failures → OPEN)
│   │   └── payment-client.ts                    Payment provider stub
│   ├── inventory-service/index.ts               Durable: per-item stock reservation + low alerts
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
    ├── order-service-flow.md                    Durable saga + SQS DLQ detail
    ├── inventory-flow.md                        Multi-item reservation + feedback loop fix
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
               v            v
  ┌──────────────┐  ┌─────────────────────────────────────────────┐
  │   DynamoDB   │  │            TrunkfulEventBus                  │
  │   Orders     │  │         (custom EventBridge bus)             │
  │   table      │  │                                              │
  │              │  │  6 rules route events to domain queues       │
  │  PENDING     │  │  and Firehose (analytics)                    │
  └──────────────┘  └──────────────────┬──────────────────────────┘
                                       │
         ┌─────────┬──────────┬────────┼────────┬──────────┐
         │         │          │        │        │          │
         v         v          v        v        v          v
  ┌──────────┐ ┌────────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌────────┐
  │  order   │ │inven-  │ │bill- │ │ful-  │ │noti- │ │Firehose│
  │  queue   │ │tory    │ │ing   │ │fill- │ │fica- │ │→ S3    │
  │          │ │queue   │ │queue │ │ment  │ │tion  │ │→ Glue  │
  │ batch:1  │ │batch:1 │ │      │ │queue │ │queue │ │→ Athena│
  └────┬─────┘ └───┬────┘ └──┬───┘ └──┬───┘ └──┬───┘ └────────┘
       │           │         │        │        │
       v           v         v        v        v
  ┌─────────┐ ┌────────┐ ┌──────┐ ┌──────┐ ┌──────┐
  │ order   │ │inven-  │ │bill- │ │ful-  │ │noti- │
  │ service │ │tory    │ │ing   │ │fill- │ │fica- │
  │(durable)│ │service │ │      │ │ment  │ │tion  │
  │         │ │(durable│ │      │ │      │ │      │
  └─────────┘ └────────┘ └──────┘ └──────┘ └──────┘
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

  ┌──────────┐  POST /orders  ┌──────────────┐  PutEvents  ┌─────────────────┐
  │  Web /   │───────────────>│ order-intake  │────────────>│ TrunkfulEventBus │
  │  Mobile  │                │   Lambda      │             │                  │
  └──────────┘                └──────┬───────┘             └────────┬──────────┘
                                     │                              │
                                     v                              │
                              ┌─────────────┐                       │
                              │  DynamoDB   │        OrderCreated   │
                              │  Orders     │        matches Rule   │
                              │  PENDING    │        1 + 2 + 6      │
                              └─────────────┘                       │
                                             ┌──────────────────────┤
                                             │                      │
                                             v                      v
                                      ┌────────────┐       ┌─────────────┐
                                      │order-queue │       │inventory-   │
                                      └─────┬──────┘       │queue        │
                                            │              └──────┬──────┘
                                            v                     v
  ┌─────────────────────────────────────────────┐  ┌──────────────────────────┐
  │  ORDER SERVICE (durable)                     │  │  INVENTORY SERVICE       │
  │                                              │  │                          │
  │  step: validate-order                        │  │  For each line item:     │
  │    DDB → VALIDATING, emit OrderValidated     │  │    step: reserve-SKU-WH  │
  │                                              │  │      DDB ADD qty: -N     │
  │  step: reserve-inventory                     │  │      emit Inventory-     │
  │    DDB → RESERVED, emit OrderReserved        │  │        Adjusted          │
  │                                              │  │                          │
  │  step: process-payment                       │  │    (if qty < 10)         │
  │    circuit breaker check → processPayment()  │  │      emit InventoryLow   │
  │                                              │  └──────────────────────────┘
  │  step: confirm-order                         │
  │    DDB → CONFIRMED, emit OrderConfirmed ─────────────────────────────────┐
  │                                              │                           │
  └──────────────────────────────────────────────┘                           │
                                                                             │
               OrderConfirmed matches Rule 3 + 4 + 5 + 6                    │
               ┌───────────────────┬───────────────────┐                     │
               v                   v                   v                     v
        ┌────────────┐     ┌─────────────┐     ┌────────────┐        ┌───────────┐
        │  BILLING   │     │ FULFILLMENT │     │NOTIFICATION│        │ Firehose  │
        │  generate  │     │  initiate   │     │  send email│        │ → S3      │
        │  invoice   │     │  shipping   │     │  (SES)     │        │ → Athena  │
        └────────────┘     └─────────────┘     └────────────┘        └───────────┘


  Order status: PENDING ──> VALIDATING ──> RESERVED ──> CONFIRMED
```

---

## Failure Path: Payment Declined

The saga catches the error, runs compensation, and returns normally.
SQS deletes the message. No retry needed.

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  ORDER SERVICE (durable)                                         │
  │                                                                  │
  │  step: validate-order        ── OK ── CHECKPOINT ✓              │
  │  step: reserve-inventory     ── OK ── CHECKPOINT ✓              │
  │  step: process-payment       ── DECLINED ── THROWS              │
  │         │                                                        │
  │         v                                                        │
  │  ┌─────────────────────────────────────────────┐                 │
  │  │  COMPENSATION                                │                 │
  │  │                                              │                 │
  │  │  1. emit InventoryReleaseRequested           │                 │
  │  │  2. DDB → FAILED                             │                 │
  │  │  3. emit OrderFailed ───────────────────────────────────────┐ │
  │  └─────────────────────────────────────────────┘               │ │
  │                                                                 │ │
  │  return { status: 'FAILED' }  ← handler succeeds, no retry     │ │
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

## Failure Path: Unhandled Crash → SQS Retry → DLQ

Lambda crashes (e.g. OOM). SQS retries 3 times. Durable checkpoints
survive across all attempts — completed steps are never re-executed.

```
  SQS: order-queue                   │
  ┌──────────────────────────────────┘
  │
  │  ATTEMPT 1 (receiveCount: 1)
  │  ┌──────────────────────────────────────────────────┐
  │  │  step: validate-order     → CHECKPOINT ✓          │
  │  │  step: reserve-inventory  → CHECKPOINT ✓          │
  │  │  step: process-payment    → ██ OOM CRASH ██       │
  │  └──────────────────────────────────────────────────┘
  │
  │  ATTEMPT 2 (receiveCount: 2)
  │  ┌──────────────────────────────────────────────────┐
  │  │  step: validate-order     → SKIP (checkpoint)     │
  │  │  step: reserve-inventory  → SKIP (checkpoint)     │
  │  │  step: process-payment    → ██ OOM CRASH ██       │
  │  └──────────────────────────────────────────────────┘
  │
  │  ATTEMPT 3 (receiveCount: 3)
  │  ┌──────────────────────────────────────────────────┐
  │  │  step: validate-order     → SKIP                  │
  │  │  step: reserve-inventory  → SKIP                  │
  │  │  step: process-payment    → ██ OOM CRASH ██       │
  │  └──────────────────────────────────────────────────┘
  │
  │  receiveCount (3) >= maxReceiveCount (3)
  │
  v
  ┌──────────────────────────────────────────────────┐
  │  SQS: order-dlq (Dead Letter Queue)               │
  │                                                    │
  │  Message parked. 14-day retention.                 │
  │  CloudWatch alarm fires → engineer investigates.   │
  │                                                    │
  │  After fix: redrive from DLQ. Lambda replays,      │
  │  SKIPS validate + reserve. Only payment runs.      │
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
              │  INVENTORY SERVICE (durable)                  │
              │                                               │
              │  Routes on detail-type:                       │
              │                                               │
              │  OrderCreated ──> per-item reservation:       │
              │    for each item:                             │
              │      step: reserve-{sku}-{warehouse}          │
              │        DDB: ADD quantity = -(ordered qty)     │
              │      step: emit-adjusted-{sku}-{warehouse}    │
              │        emit InventoryAdjusted (→ analytics)   │
              │      if qty < 10:                             │
              │        step: emit-low-{sku}-{warehouse}       │
              │          emit InventoryLow (→ notification)   │
              │                                               │
              │  InventoryReceived / ReturnInitiated ──>      │
              │    step: adjust-inventory                     │
              │      DDB: ADD quantity = +N                   │
              │    step: emit-adjusted                        │
              │      emit InventoryAdjusted (→ analytics)     │
              │    if qty < 10:                               │
              │      step: emit-low-stock                     │
              │        emit InventoryLow (→ notification)     │
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
  │   try/catch + saga compensation                                 │
  │   ├─ Payment declined? → release inventory, mark FAILED         │
  │   ├─ Validation fails? → mark FAILED, emit OrderFailed          │
  │   └─ Handles EXPECTED business failures gracefully              │
  │                                                                 │
  │   ┌─────────────────────────────────────────────────────────┐   │
  │   │                                                         │   │
  │   │   Layer 2: DURABLE EXECUTION                            │   │
  │   │   context.step() + checkpointing                        │   │
  │   │   ├─ Timeout mid-saga? → replay from last checkpoint    │   │
  │   │   ├─ Transient failure? → step retried, prior skipped   │   │
  │   │   └─ Handles INFRASTRUCTURE failures within invocation  │   │
  │   │                                                         │   │
  │   │   ┌─────────────────────────────────────────────────┐   │   │
  │   │   │                                                 │   │   │
  │   │   │   Layer 3: SQS RETRY + DLQ                      │   │   │
  │   │   │   maxReceiveCount: 3 + dead letter queue         │   │   │
  │   │   │   ├─ Lambda crashes? → SQS retries 3x            │   │   │
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
