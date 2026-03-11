# Event Routing: EventBridge Rules

## Event Types → Rules → Queues

```
                                ┌───────────────────────────┐
                                │      TrunkfulEventBus     │
                                │  (custom EventBridge bus) │
                                │                           │
                                │   source: trunkful.orders │
                                └─────────────┬─────────────┘
                                              │
               All events enter the bus.      │      6 rules evaluate
               Each rule pattern-matches      │      INDEPENDENTLY and
               on detail-type.                │      in PARALLEL.
                                              │
       ┌──────────┬──────────┬────────────────┼──────────┬──────────┐
       │          │          │                │          │          │
       v          v          v                v          v          v
  ┌─────────┐┌─────────┐┌─────────┐   ┌─────────┐┌─────────┐┌─────────┐
  │ Rule 1  ││ Rule 2  ││ Rule 3  │   │ Rule 4  ││ Rule 5  ││ Rule 6  │
  │OrderCre-││Inventory││OrderCon-│   │OrderCon-││Notifica-││AllEvents│
  │ated→    ││Events→  ││firmed→  │   │firmed→  ││tionEvts→││→Fire-  │
  │OrderQ   ││Invent-  ││BillingQ │   │Fulfill- ││Notifica-││hose    │
  │         ││oryQ     ││         │   │mentQ    ││tionQ    ││        │
  │ matches ││ matches ││ matches │   │ matches ││ matches ││ matches│
  │ detail- ││ detail- ││ detail- │   │ detail- ││ detail- ││ALL from│
  │ type:   ││ type:   ││ type:   │   │ type:   ││ type:   ││source: │
  │         ││         ││         │   │         ││         ││trunkful│
  │ Order   ││ Order   ││ Order   │   │ Order   ││ Order   ││.orders │
  │ Created ││ Created ││Confirmed│   │Confirmed││Confirmed││        │
  │         ││Inventory││         │   │         ││ Order   ││        │
  │         ││ Received││         │   │         ││ Failed  ││        │
  │         ││ Return  ││         │   │         ││Inventory││        │
  │         ││Initiated││         │   │         ││ Low     ││        │
  └────┬────┘└────┬────┘└────┬────┘   └────┬────┘└────┬────┘└────┬────┘
       │          │          │              │          │          │
       v          v          v              v          v          v
  ┌─────────┐┌─────────┐┌─────────┐  ┌─────────┐┌─────────┐┌─────────┐
  │ order   ││inventory││ billing │  │fulfill- ││notific- ││Firehose │
  │ queue   ││ queue   ││ queue   │  │ment     ││ation    ││→ S3     │
  │         ││         ││         │  │ queue   ││ queue   ││→ Glue   │
  │ batch:1 ││ batch:1 ││ batch:5 │  │ batch:5 ││ batch:5 ││→ Athena │
  └────┬────┘└────┬────┘└────┬────┘  └────┬────┘└────┬────┘└─────────┘
       │          │          │             │          │
       │ SQS poll │ SQS poll │ SQS poll    │ SQS poll│ SQS poll
       v          v          v             v          v
  ┌─────────┐┌─────────┐┌─────────┐  ┌─────────┐┌─────────┐
  │ order-  ││inventory││ billing │  │fulfill- ││notific- │
  │ saga    ││-workflow││ lambda  │  │ment     ││ation    │
  │ trigger ││ trigger ││ (direct)│  │ lambda  ││ lambda  │
  │ lambda  ││ lambda  ││         │  │ (direct)││ (direct)│
  └────┬────┘└────┬────┘└─────────┘  └─────────┘└─────────┘
       │          │
       │ Start    │ Start
       │ Execution│ Execution
       v          v
  ┌─────────┐┌──────────┐
  │ Order   ││ Inventory│
  │ Saga SM ││ Workflow │
  │(Step Fn)││SM(StepFn)│
  └─────────┘└──────────┘
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

## Rule Reference

| Rule | Title                          | detail-type matches                              | Target            |
|------|--------------------------------|--------------------------------------------------|-------------------|
| 1    | OrderCreated→OrderQueue        | OrderCreated                                     | Order Queue       |
| 2    | InventoryEvents→InventoryQueue | OrderCreated, InventoryReceived, ReturnInitiated | Inventory Queue   |
| 3    | OrderConfirmed→BillingQueue    | OrderConfirmed                                   | Billing Queue     |
| 4    | OrderConfirmed→FulfillmentQueue| OrderConfirmed                                   | Fulfillment Queue |
| 5    | NotificationEvents→NotificationQueue | OrderConfirmed, OrderFailed, InventoryLow  | Notification Queue|
| 6    | AllEvents→Firehose             | ALL (source: trunkful.orders)                    | Firehose → S3     |

## Step Functions vs. Direct Consumers

**Trigger lambda → Step Functions (saga / workflow):**
- Order Queue (batch:1) → order-saga trigger lambda → StartExecution → Order Saga SM
- Inventory Queue (batch:1) → inventory-workflow trigger lambda → StartExecution → Inventory Workflow SM

**Direct SQS consumers (no Step Functions):**
- Billing Queue (batch:5) → billing lambda
- Fulfillment Queue (batch:5) → fulfillment lambda
- Notification Queue (batch:5) → notification lambda

## Notes

InventoryAdjusted is intentionally NOT routed to the inventory queue.
It is a notification event ("this happened"), not a command ("do this").
Routing it back would create an infinite feedback loop and double-count
adjustments. It flows only to Firehose (Rule 6: AllEvents→Firehose) for analytics.

OrderCreated fans out to BOTH the order queue (Rule 1: OrderCreated→OrderQueue)
AND the inventory queue (Rule 2: InventoryEvents→InventoryQueue) simultaneously.
Each queue has a trigger lambda that calls StartExecution on its respective Step
Functions state machine — the Order Saga SM and the Inventory Workflow SM.

OrderConfirmed fans out to THREE queues in parallel: billing
(Rule 3: OrderConfirmed→BillingQueue), fulfillment
(Rule 4: OrderConfirmed→FulfillmentQueue), and notification
(Rule 5: NotificationEvents→NotificationQueue). These three consumers are
direct SQS-polling lambdas — no Step Functions involved.
