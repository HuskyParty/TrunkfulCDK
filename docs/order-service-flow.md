# Order Service: Step Functions Saga + SQS DLQ Flow

## Full Message Flow

```
  EventBridge                    SQS                     Lambda                  Step Functions
  (TrunkfulEventBus)       (order-queue)           (order-saga-trigger)       (order-saga)

 +----------------+      +-------------------+      +------------------+  +--------------------------+
 | OrderCreated   |----->|                   |----->| order-saga-      |->|  Order Saga              |
 |   event        | Rule |  order-queue      | Poll | trigger Lambda   |SM|  State Machine           |
 +----------------+  #1  |                   |      |                  |  |                          |
   Rule 1:            :   |  visibilityTimeout|      |  Extracts detail |  |  ValidateOrder (Lambda)  |
   OrderCreated       :   |  = 60s            |      |  from EventBridge|  |       |                  |
   -> OrderQueue      :   |                   |      |  envelope, calls |  |       v                  |
                          |  maxReceiveCount  |      |  StartExecution  |  |  ReserveInventory        |
                          |  = 3              |      |                  |  |       |                  |
                          |                   |      +------------------+  |       v                  |
                          |                   |                            |  ProcessPayment          |
                          |                   |                            |    (circuit breaker)     |
                          |                   |                            |       |                  |
                          |                   |                            |       v                  |
                          |                   |                            |  ConfirmOrder            |
                          |                   |                            |    emit OrderConfirmed   |
                          +-------------------+                            +--------------------------+
```

## Step Functions State Machine: Order Saga

```
  +----------------------------------------------------------------------+
  |  Order Saga State Machine                                            |
  |                                                                      |
  |  +-------------------+                                               |
  |  | ValidateOrder     |---- Catch --> MarkFailedEarly                 |
  |  | (Lambda)          |                                               |
  |  | - validate fields |                                               |
  |  | - DDB: PENDING ->  |                                               |
  |  |   VALIDATING      |                                               |
  |  | - emit             |                                               |
  |  |   OrderValidated  |                                               |
  |  +---------+---------+                                               |
  |            | success                                                 |
  |            v                                                         |
  |  +-------------------+                                               |
  |  | ReserveInventory  |---- Catch --> MarkFailedEarly                 |
  |  | (Lambda)          |                                               |
  |  | - DDB: VALIDATING  |                                               |
  |  |   -> RESERVED     |                                               |
  |  | - emit             |                                               |
  |  |   OrderReserved   |                                               |
  |  +---------+---------+                                               |
  |            | success                                                 |
  |            v                                                         |
  |  +-------------------+                                               |
  |  | ProcessPayment    |---- Catch --> CompensateRelease -> MarkFailed |
  |  | (Lambda)          |                                               |
  |  | - circuit breaker  |                                               |
  |  |   check (DDB)     |                                               |
  |  | - call payment svc |                                               |
  |  | - no DDB status    |                                               |
  |  |   change          |                                               |
  |  +---------+---------+                                               |
  |            | success                                                 |
  |            v                                                         |
  |  +-------------------+                                               |
  |  | ConfirmOrder      |---- Catch --> CompensateRelease -> MarkFailed |
  |  | (Lambda)          |                                               |
  |  | - DDB: RESERVED    |                                               |
  |  |   -> CONFIRMED    |                                               |
  |  | - emit             |                                               |
  |  |   OrderConfirmed  |                                               |
  |  +---------+---------+                                               |
  |            | success                                                 |
  |            v                                                         |
  |        (Succeed)                                                     |
  |                                                                      |
  |  ------------------------------------------------------------------- |
  |  Compensation States:                                                |
  |                                                                      |
  |  MarkFailedEarly:        DDB -> FAILED, emit OrderFailed            |
  |  CompensateRelease:      emit InventoryReleaseRequested              |
  |  MarkFailed (after comp): DDB -> FAILED, emit OrderFailed           |
  +----------------------------------------------------------------------+
```

Each saga step is a separate Lambda function invoked by Step Functions.
Step Functions provides native per-state retry (2 attempts with exponential
backoff) before the Catch handler fires.

## Catch-Based Compensation Logic

Errors at different stages route to different compensation paths depending on
whether inventory has been reserved yet:

| Failing state      | Catch target        | Compensation path                             |
|--------------------|---------------------|-----------------------------------------------|
| ValidateOrder      | MarkFailedEarly     | DDB -> FAILED, emit OrderFailed               |
| ReserveInventory   | MarkFailedEarly     | DDB -> FAILED, emit OrderFailed               |
| ProcessPayment     | CompensateRelease   | emit InventoryReleaseRequested -> MarkFailed   |
| ConfirmOrder       | CompensateRelease   | emit InventoryReleaseRequested -> MarkFailed   |

- **Before inventory is reserved** (ValidateOrder, ReserveInventory): errors go
  straight to `MarkFailedEarly`, which marks the order FAILED in DynamoDB and
  emits `OrderFailed`. No inventory release is needed because the reservation
  either was never attempted or did not complete.
- **After inventory is reserved** (ProcessPayment, ConfirmOrder): errors go to
  `CompensateRelease`, which emits `InventoryReleaseRequested` so the inventory
  service can undo the reservation, then chains to `MarkFailed` (the
  `MarkFailedAfterComp` state in CDK), which marks the order FAILED and emits
  `OrderFailed`.

## Built-in Retry per Step

```
  Step Functions retries transient errors before catching:

  ProcessPayment (Lambda)
    |
    +-- Attempt 1: States.TaskFailed -> RETRY (wait 1s)
    +-- Attempt 2: States.TaskFailed -> RETRY (wait 2s)
    +-- Attempt 3: States.TaskFailed -> CATCH -> CompensateRelease -> MarkFailed

  After maxAttempts (2 retries), the error flows to the Catch handler.
  Completed earlier steps (Validate, Reserve) are NOT re-executed --
  Step Functions tracks state per execution.
```

Every step (ValidateOrder, ReserveInventory, ProcessPayment, ConfirmOrder) uses
the same retry configuration:

| Parameter     | Value              |
|---------------|--------------------|
| errors        | States.TaskFailed  |
| maxAttempts   | 2                  |
| interval      | 1 second           |
| backoffRate   | 2                  |

This means each step gets up to 3 total attempts (1 initial + 2 retries) with
waits of 1s and 2s before the error falls through to the Catch handler.

## Circuit Breaker on ProcessPayment

ProcessPayment is protected by a DynamoDB-backed circuit breaker stored in the
Orders table under the key pattern `CIRCUIT#payment`.

| Parameter          | Value       |
|--------------------|-------------|
| Failure threshold  | 5           |
| Failure window     | 60 seconds  |
| Cooldown (OPEN)    | 30 seconds  |

**States**: CLOSED (healthy) -> OPEN (tripped) -> HALF_OPEN (probing)

- When 5 failures occur within a 60-second window, the circuit **opens**.
- While OPEN, ProcessPayment immediately throws without calling the payment
  service. This protects the downstream service from cascading load.
- After 30 seconds of cooldown, the circuit transitions to HALF_OPEN, allowing
  a single probe request through.
- A successful probe resets the circuit to CLOSED. A failed probe re-opens it.

The circuit breaker state is read/written via `GetItem`/`PutItem` on the Orders
DynamoDB table (key: `pk=CIRCUIT#payment, sk=CIRCUIT#payment`).

## Event Emissions

Each step in the saga emits events to the TrunkfulEventBus (source:
`trunkful.orders`):

| Step                | Event emitted               | Payload includes              |
|---------------------|-----------------------------|-------------------------------|
| ValidateOrder       | OrderValidated              | orderId                       |
| ReserveInventory    | OrderReserved               | orderId, items                |
| ProcessPayment      | *(none)*                    | --                            |
| ConfirmOrder        | OrderConfirmed              | orderId, customerId, totalAmount, transactionId |
| CompensateRelease   | InventoryReleaseRequested   | orderId, items                |
| MarkFailedEarly     | OrderFailed                 | orderId, reason               |
| MarkFailed          | OrderFailed                 | orderId, reason               |

## DynamoDB Order Status Transitions

The Orders table tracks progress through the saga. Status is updated by each
step Lambda using `UpdateItem`:

```
  Happy path:   PENDING -> VALIDATING -> RESERVED -> CONFIRMED
  Failure path: PENDING -> VALIDATING -> RESERVED -> FAILED
                PENDING -> VALIDATING -> FAILED           (if ValidateOrder or
                PENDING -> FAILED                          ReserveInventory fails)
```

Note: ProcessPayment does not update the order status in DynamoDB. It only
interacts with the circuit breaker state and the external payment service.

## SQS Retry + DLQ Escalation

```
                    +----------------------------------------+
                    |          SQS: order-queue               |
                    |                                         |
                    |  receiveCount tracks per-message        |
                    |  delivery attempts                      |
                    +--------------+-------------------------+
                                   |
                        Trigger Lambda polls message
                                   |
             +---------------------+---------------------+
             |                     |                     |
        Attempt 1             Attempt 2             Attempt 3
             |                     |                     |
    +--------+--------+   +--------+--------+   +-------+---------+
    | Trigger Lambda  |   | Trigger Lambda  |   | Trigger Lambda   |
    | -> StartExec    |   | -> StartExec    |   | -> StartExec     |
    |                 |   |                 |   |                  |
    |  SM fails (e.g. |   |  SM fails again |   |  SM fails again  |
    |  payment svc    |   |                 |   |                  |
    |  is down)       |   |                 |   |                  |
    +-----------------+   +-----------------+   +--------+---------+
                                                         |
                                              receiveCount = 3
                                              maxReceiveCount = 3
                                                         |
                                                         v
                                          +--------------------------+
                                          |    SQS: order-dlq        |
                                          |    (Dead Letter Queue)   |
                                          |                          |
                                          |  Message parked here     |
                                          |  for investigation.      |
                                          |  14-day retention.       |
                                          |                          |
                                          |  CloudWatch alarm fires  |
                                          |  when messages > 0       |
                                          +--------------------------+
```

## Why SQS Sits in Front of Step Functions

Step Functions does not have built-in buffering. Without SQS, a burst of
10K OrderCreated events would start 10K concurrent state machine executions.
The SQS queue provides:

1. **Buffering** -- absorbs traffic spikes so downstream systems are not
   overwhelmed. Lambda polls at a controlled rate (batchSize: 1).
2. **Backpressure** -- if the trigger Lambda or Step Functions is slow,
   messages stay in the queue rather than being dropped.
3. **DLQ semantics** -- failed messages land in a dead-letter queue after 3
   attempts (maxReceiveCount: 3, 14-day retention).
4. **Redrive** -- engineers can re-process DLQ messages after fixing the
   root cause.

The trigger Lambda (`order-saga-trigger`) is a thin pass-through that extracts
the order from the SQS/EventBridge envelope and calls `StartExecution` on the
state machine.

## EventBridge Rules Reference

The following rules on the TrunkfulEventBus route events to SQS queues and
analytics:

| Rule | Title                          | Event pattern                                           | Target            |
|------|--------------------------------|---------------------------------------------------------|-------------------|
| Rule 1 | OrderCreated -> OrderQueue         | source: trunkful.orders, detailType: OrderCreated       | order-queue       |
| Rule 2 | InventoryEvents -> InventoryQueue  | source: trunkful.orders, detailType: OrderCreated, InventoryReceived, ReturnInitiated | inventory-queue   |
| Rule 3 | OrderConfirmed -> BillingQueue     | source: trunkful.orders, detailType: OrderConfirmed     | billing-queue     |
| Rule 4 | OrderConfirmed -> FulfillmentQueue | source: trunkful.orders, detailType: OrderConfirmed     | fulfillment-queue |
| Rule 5 | NotificationEvents -> NotificationQueue | source: trunkful.orders, detailType: OrderConfirmed, OrderFailed, InventoryLow | notification-queue |
| Rule 6 | AllEvents -> Firehose            | source: trunkful.orders (all events)                    | Kinesis Firehose delivery stream |
