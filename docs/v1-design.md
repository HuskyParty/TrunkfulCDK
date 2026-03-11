# TrunkfulCDK — Event-Driven Inventory & Order Processing System

AWS CDK (TypeScript) implementation of an event-driven order processing architecture for a mid-size retailer with 5 ingestion channels. Order orchestration uses AWS Step Functions state machines (saga pattern with Catch-based compensation) instead of the earlier durable Lambda SDK.

## Project Structure

```
TrunkfulCDK/
├── bin/trunkful.ts                           # CDK app entry point
├── lib/
│   ├── trunkful-stack.ts                     # Main stack — composes constructs + EventBridge rules
│   ├── constructs/
│   │   ├── data-layer.ts                     # DynamoDB: Orders, Inventory, Idempotency tables
│   │   ├── event-bus.ts                      # EventBridge custom bus + archive
│   │   ├── queues.ts                         # 5 SQS queues + 5 DLQs
│   │   ├── ingestion-api.ts                  # API GW, Cognito, WAF, intake Lambdas, DDB direct read
│   │   ├── ingestion-iot.ts                  # IoT Core topic rule + POS intake Lambda (L1)
│   │   ├── ingestion-s3.ts                   # S3 upload bucket + admin ingest Lambda
│   │   ├── ingestion-warehouse.ts            # IAM role for direct PutEvents
│   │   ├── processing.ts                     # Step Functions state machines (Order Saga SM, Inventory Workflow SM) + trigger Lambdas + Billing, Fulfillment, Notification Lambdas
│   │   ├── analytics.ts                      # Firehose → S3 data lake → Glue → Athena
│   │   ├── security.ts                       # KMS CMK for PII, Secrets Manager for payment key
│   │   └── monitoring.ts                     # CloudWatch dashboard + alarms
│   └── shared/
│       └── lambda-defaults.ts                # Common Lambda config helper
├── lambda/
│   ├── shared/
│   │   ├── idempotency.ts                    # DDB conditional PutItem idempotency check
│   │   ├── event-emitter.ts                  # EventBridge PutEvents helper
│   │   ├── logger.ts                         # Structured JSON logger
│   │   └── types.ts                          # Shared types/enums
│   ├── order-intake/index.ts                 # Web/Mobile → API GW handler
│   ├── pos-intake/index.ts                   # POS → IoT Core handler
│   ├── webhook-intake/index.ts               # Supplier webhook handler
│   ├── admin-ingest/index.ts                 # S3 trigger handler
│   ├── order-service/
│   │   ├── circuit-breaker.ts                # DDB-backed circuit breaker (used by process-payment step)
│   │   └── payment-client.ts                 # Payment provider stub (used by process-payment step)
│   ├── order-steps/                          # Order saga step Lambdas (invoked by Order Saga SM)
│   │   ├── validate.ts                       # Step 1: validate order fields
│   │   ├── reserve-inventory.ts              # Step 2: mark RESERVED in Orders table
│   │   ├── process-payment.ts                # Step 3: circuit breaker + payment via payment-client
│   │   ├── confirm-order.ts                  # Step 4: mark CONFIRMED, emit OrderConfirmed
│   │   ├── release-inventory.ts              # Compensation: release reservation on failure
│   │   ├── mark-failed.ts                    # Terminal: mark FAILED, emit OrderFailed
│   │   ├── trigger.ts                        # Thin SQS → sfn.StartExecution bridge Lambda
│   │   └── shared.ts                         # DDB/EventBridge helpers for step Lambdas
│   ├── inventory-steps/                      # Inventory workflow step Lambdas (invoked by Inventory Workflow SM)
│   │   ├── process-item.ts                   # Single item adjustment with idempotency
│   │   └── trigger.ts                        # Thin SQS → sfn.StartExecution bridge Lambda
│   ├── billing/index.ts                      # Billing stub
│   ├── fulfillment/index.ts                  # Fulfillment stub
│   └── notification/index.ts                 # SES email + SNS SMS + KMS decrypt
├── test/
│   └── trunkful-stack.test.ts                # Synth + resource assertions (17 tests)
├── docs/
│   └── cdk-getting-started.md                # CDK useful commands reference
├── package.json                              # @aws-sdk/client-sfn (no durable-execution-sdk)
├── tsconfig.json
└── cdk.json
```

## Quick Start

```bash
npm install          # Install dependencies
npm run build        # Compile TypeScript
npm test             # Run 17 assertion tests
npx cdk synth        # Generate CloudFormation template
npx cdk deploy       # Deploy to AWS (requires bootstrapped account)
```

## Architecture Overview

- **5 Ingestion Channels**: Web/Mobile (API GW + Cognito), POS (IoT Core), Supplier Webhooks, Warehouse Scanners (direct PutEvents), Admin Bulk Upload (S3)
- **Event Bus**: Custom EventBridge bus routes events to domain-specific SQS queues via 6 rules
- **Processing**: Two Step Functions state machines orchestrate core workflows; Billing, Fulfillment, and Notification Lambdas consume downstream queues directly
- **CQRS Read Path**: GET /orders/{orderId} uses API Gateway → DynamoDB direct integration (no Lambda)
- **Analytics**: Firehose → S3 data lake → Glue catalog → Athena queries
- **Security**: KMS CMK for PII encryption, Cognito auth, WAF with rate limiting + managed rule sets
- **Observability**: CloudWatch dashboard, DLQ alarms, X-Ray tracing on all Lambdas and state machines

## EventBridge Routing Rules

| Rule | Source Events | Target Queue |
|------|--------------|--------------|
| Rule 1: OrderCreated→OrderQueue | OrderCreated | Order Queue |
| Rule 2: InventoryEvents→InventoryQueue | OrderCreated, InventoryReceived, ReturnInitiated | Inventory Queue |
| Rule 3: OrderConfirmed→BillingQueue | OrderConfirmed | Billing Queue |
| Rule 4: OrderConfirmed→FulfillmentQueue | OrderConfirmed | Fulfillment Queue |
| Rule 5: NotificationEvents→NotificationQueue | OrderConfirmed, OrderFailed, InventoryLow | Notification Queue |
| Rule 6: AllEvents→Firehose | All events | Firehose (analytics) |

## Processing Layer — Step Functions State Machines

The `processing.ts` construct defines two Step Functions state machines plus three downstream Lambdas. SQS queues remain in front of each state machine for buffering and dead-letter support; thin trigger Lambdas bridge `SQS → sfn.StartExecution`.

### Order Saga State Machine (`OrderSagaSM`)

Implements the saga pattern with Catch-based compensation. The happy path is a linear chain of four LambdaInvoke tasks:

```
ValidateOrder → ReserveInventory → ProcessPayment → ConfirmOrder
```

**Compensation logic (Catch handlers):**

- **Before inventory is reserved** (ValidateOrder, ReserveInventory fail): Catch → `MarkFailedEarly` — marks the order FAILED.
- **After inventory is reserved** (ProcessPayment, ConfirmOrder fail): Catch → `CompensateRelease` (release-inventory) → `MarkFailedAfterComp` (mark-failed) — releases the reservation, then marks the order FAILED.

**Per-step retry:** Every main step retries `States.TaskFailed` up to 2 times with exponential backoff (interval 1 s, rate 2).

**State machine settings:** 5-minute timeout, X-Ray tracing enabled.

**Step Lambdas** (in `lambda/order-steps/`):

| Lambda | Purpose |
|--------|---------|
| `validate.ts` | Validate order fields against Orders table |
| `reserve-inventory.ts` | Mark items RESERVED in Orders table, emit event |
| `process-payment.ts` | Run DDB-backed circuit breaker, call payment-client, update order |
| `confirm-order.ts` | Mark order CONFIRMED, emit OrderConfirmed |
| `release-inventory.ts` | Compensation — release reservation, emit event |
| `mark-failed.ts` | Terminal — mark order FAILED, emit OrderFailed |
| `trigger.ts` | SQS event source (batchSize 1) → StartExecution with message body as input |
| `shared.ts` | DDB/EventBridge helpers shared across step Lambdas |

Supporting files retained in `lambda/order-service/`: `circuit-breaker.ts` (DDB-backed circuit breaker) and `payment-client.ts` (payment provider stub), both imported by `process-payment.ts`.

### Inventory Workflow State Machine (`InventoryWorkflowSM`)

Handles OrderCreated reservation and generic inventory adjustments (InventoryReceived, ReturnInitiated).

```
NormalizeInput (Pass) → Choice (InventoryEventType)
  ├── detailType == "OrderCreated" → MapOrderItems (Map per-item) → ProcessReservationItem (LambdaInvoke)
  └── otherwise → ExtractGenericDetail (Pass) → ProcessGenericItem (LambdaInvoke)
```

- **NormalizeInput** — Pass state that copies `detail-type` (hyphenated EventBridge field) into `detailType` for use in Choice conditions.
- **MapOrderItems** — Map state iterates over `$.detail.items`, passing `{sku, quantity, orderId, isReservation: true}` to `process-item.ts`.
- **Generic branch** — Passes `$.detail` directly to `process-item.ts` for InventoryReceived / ReturnInitiated adjustments.

**State machine settings:** 5-minute timeout, X-Ray tracing enabled.

**Step Lambdas** (in `lambda/inventory-steps/`):

| Lambda | Purpose |
|--------|---------|
| `process-item.ts` | Single-item inventory adjustment with DDB idempotency check; emits InventoryLow when threshold breached |
| `trigger.ts` | SQS event source (batchSize 1) → StartExecution with message body as input |

### Downstream Lambdas (SQS-driven, not Step Functions)

| Lambda | Queue | Batch Size |
|--------|-------|------------|
| `billing/index.ts` | Billing Queue (Rule 3: OrderConfirmed→BillingQueue) | 5 |
| `fulfillment/index.ts` | Fulfillment Queue (Rule 4: OrderConfirmed→FulfillmentQueue) | 5 |
| `notification/index.ts` | Notification Queue (Rule 5: NotificationEvents→NotificationQueue) | 5 |

## Resilience Model — Three Layers

| Layer | Mechanism | Scope |
|-------|-----------|-------|
| **1. Application logic** | Step Functions Catch handlers + saga compensation (release-inventory → mark-failed) | Order Saga SM |
| **2. Step Functions orchestration** | Per-state `addRetry` — retries `States.TaskFailed` up to 2 times with exponential backoff | All LambdaInvoke tasks in Order Saga SM |
| **3. SQS retry + DLQ** | SQS delivery retries + dead-letter queues on every queue; trigger Lambda failures return messages to queue | All queues (Order, Inventory, Billing, Fulfillment, Notification) |

## Migration from Durable Lambda SDK

The original design used `@aws/durable-execution-sdk-js` to orchestrate the order saga inside a single monolithic Lambda (`lambda/order-service/index.ts`) and inventory processing inside another (`lambda/inventory-service/index.ts`). These were replaced with:

- **Two Step Functions state machines** defined in `lib/constructs/processing.ts` using CDK's `aws-stepfunctions` and `aws-stepfunctions-tasks` modules.
- **Dedicated step Lambdas** in `lambda/order-steps/` and `lambda/inventory-steps/`, each responsible for a single task.
- **Thin trigger Lambdas** (`trigger.ts` in each directory) that bridge SQS → `sfn.StartExecution` using `@aws-sdk/client-sfn`.
- The `@aws/durable-execution-sdk-js` dependency was removed from `package.json`; `@aws-sdk/client-sfn` was added.
- `lambda/order-service/index.ts` and `lambda/inventory-service/index.ts` were deleted. `circuit-breaker.ts` and `payment-client.ts` remain in `lambda/order-service/` as they are imported by the `process-payment` step Lambda.
