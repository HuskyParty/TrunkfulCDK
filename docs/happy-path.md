# Happy Path: Order Placed → Confirmed

```
  CUSTOMER                    INGESTION                         EVENT BUS
  ════════                    ═════════                         ═════════

  ┌──────────┐   POST /orders   ┌──────────────┐                ┌─────────────────┐
  │  Web /   │─────────────────>│ order-intake  │──PutEvents───>│ TrunkfulEventBus │
  │  Mobile  │   (Cognito auth) │   Lambda      │ (OrderCreated)│                  │
  └──────────┘                  └──────┬───────┘               └────────┬──────────┘
                                      │                                │
                         ┌────────────┘                                │
                         │ DDB PutItem                                 │
                         v (status: PENDING)                           │
                  ┌─────────────┐                                      │
                  │  DynamoDB   │                                      │
                  │  Orders     │  OrderCreated event matches:         │
                  │             │    Rule 1: OrderCreated→OrderQueue   │
                  │             │    Rule 2: InventoryEvents→InventoryQueue │
                  │  status:    │    Rule 6: AllEvents→Firehose        │
                  │  PENDING    │                                      │
                  └─────────────┘                                      │
                                  ┌────────────────────────────────────┤
                                  │                                    │
                                  │ Rule 1: OrderCreated→OrderQueue   │ Rule 2: InventoryEvents→InventoryQueue
                                  v                                    v
                           ┌────────────┐                    ┌─────────────┐
                           │order-queue │                    │inventory-   │
                           │   (SQS)    │                    │queue (SQS)  │
                           └─────┬──────┘                    └──────┬──────┘
                                 │ SQS poll                         │ SQS poll
                                 v                                  v
                           ┌────────────┐                    ┌─────────────┐
                           │ order-saga │                    │ inventory-  │
                           │ trigger λ  │                    │ workflow    │
                           └─────┬──────┘                    │ trigger λ   │
                                 │                           └──────┬──────┘
                                 │ StartExecution                   │ StartExecution
                                 v                                  v
  ┌──────────────────────────────────────────┐  ┌─────────────────────────────────────┐
  │  Order Saga SM (Step Functions)          │  │  Inventory Workflow SM (Step Fns)   │
  │                                          │  │                                     │
  │  ValidateOrder (Lambda)                  │  │  NormalizeInput (Pass)              │
  │    ├─ validate fields                    │  │       │                             │
  │    ├─ DDB UpdateItem → VALIDATING        │  │       v                             │
  │    └─ PutEvents → OrderValidated         │  │  Choice: detailType?               │
  │                                          │  │    ┌──────────┬──────────┐          │
  │  ReserveInventory (Lambda)               │  │    │ OrderCreated        │ otherwise │
  │    ├─ DDB UpdateItem → RESERVED          │  │    v                    v           │
  │    └─ PutEvents → OrderReserved          │  │  Map (per item):    ExtractDetail  │
  │                                          │  │    ProcessItem λ     (Pass)        │
  │  ProcessPayment (Lambda)                 │  │      ├─ DDB ADD        │           │
  │    ├─ checkCircuit('payment') → CLOSED   │  │      │  qty: -N        v           │
  │    ├─ processPayment() → OK             │  │      ├─ PutEvents → ProcessItem λ  │
  │    └─ recordSuccess('payment')           │  │      │  InventoryAdjusted          │
  │                                          │  │      └─ (if qty < 10)              │
  │  ConfirmOrder (Lambda)                   │  │         PutEvents →                │
  │    ├─ DDB UpdateItem → CONFIRMED         │  │         InventoryLow               │
  │    └─ PutEvents → OrderConfirmed ──────────────────────────────────────────────┐ │
  │                                          │  └─────────────────────────────────────┘
  └──────────────────────────────────────────┘                                     │
                                                                                   │
    OrderConfirmed ──> EventBridge ──> matches:                                    │
      Rule 3: OrderConfirmed→BillingQueue                                          │
      Rule 4: OrderConfirmed→FulfillmentQueue                                      │
      Rule 5: NotificationEvents→NotificationQueue                                 │
      Rule 6: AllEvents→Firehose                                                   │
                                                                                   │
         ┌──────────────────┬──────────────────┬──────────────────┐                │
         │ Rule 3           │ Rule 4           │ Rule 5           │ Rule 6         │
         v                  v                  v                  v                 │
  ┌────────────┐     ┌─────────────┐    ┌────────────┐    ┌─────────┐              │
  │billing-    │     │fulfillment- │    │notification│    │Firehose │──────────────┘
  │queue (SQS) │     │queue (SQS)  │    │-queue (SQS)│    │→ S3     │
  └─────┬──────┘     └──────┬──────┘    └─────┬──────┘    └─────────┘
        │ SQS poll          │ SQS poll        │ SQS poll
        v                   v                  v
  ┌───────────┐     ┌─────────────┐     ┌────────────────┐
  │  BILLING  │     │ FULFILLMENT │     │  NOTIFICATION  │
  │  LAMBDA   │     │  LAMBDA     │     │  LAMBDA        │
  │           │     │             │     │                │
  │  Generate │     │  Initiate   │     │  Send confirm  │
  │  invoice  │     │  shipping   │     │  email (SES)   │
  └───────────┘     └─────────────┘     └────────────────┘


  ════════════════════════════════════════════════════════════════════════
  ORDER STATUS TIMELINE (DynamoDB UpdateItem at each step)
  ════════════════════════════════════════════════════════════════════════

  PENDING ──> VALIDATING ──> RESERVED ──> CONFIRMED
     │            │              │             │
   intake      Step Fn        Step Fn       Step Fn
   lambda      ValidateOrder  ReserveInv.   ConfirmOrder
```
