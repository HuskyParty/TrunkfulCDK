# Happy Path: Order Placed → Confirmed

```
  CUSTOMER                    INGESTION                         EVENT BUS
  ════════                    ═════════                         ═════════

  ┌──────────┐   POST /orders   ┌──────────────┐   PutEvents   ┌─────────────────┐
  │  Web /   │─────────────────>│ order-intake  │─────────────>│ TrunkfulEventBus │
  │  Mobile  │   (Cognito auth) │   Lambda      │              │                  │
  └──────────┘                  └──────┬───────┘              └────────┬──────────┘
                                       │                               │
                          ┌────────────┘                               │
                          v                                            │
                   ┌─────────────┐                                     │
                   │  DynamoDB   │                                     │
                   │  Orders     │                                     │
                   │             │                                     │
                   │  status:    │           ┌──────────────────────────┘
                   │  PENDING    │           │
                   └─────────────┘           │  OrderCreated event
                                             │  matches Rule 1 + Rule 2 + Rule 6
                          ┌──────────────────┼──────────────────┐
                          │                  │                  │
                          v                  v                  v
                   ┌────────────┐    ┌─────────────┐    ┌───────────┐
                   │order-queue │    │inventory-   │    │ Firehose  │
                   │            │    │queue        │    │ → S3      │
                   └─────┬──────┘    └──────┬──────┘    └───────────┘
                         │                  │
                         v                  v
  ┌──────────────────────────────┐  ┌────────────────────────────┐
  │  ORDER SERVICE (durable)     │  │  INVENTORY SERVICE (durable)│
  │                              │  │                             │
  │  step: validate-order        │  │  For each item in order:   │
  │    ├─ validate fields        │  │    step: reserve-SKU-WH    │
  │    ├─ DDB → VALIDATING       │  │      ├─ DDB ADD qty: -N   │
  │    └─ emit OrderValidated    │  │      └─ emit Inventory-   │
  │                              │  │           Adjusted         │
  │  step: reserve-inventory     │  │                             │
  │    ├─ DDB → RESERVED         │  │    (if qty < 10)           │
  │    └─ emit OrderReserved     │  │      └─ emit InventoryLow │
  │                              │  │                             │
  │  step: process-payment       │  └────────────────────────────┘
  │    ├─ circuit breaker: CLOSED│
  │    ├─ processPayment() → OK  │
  │    └─ recordSuccess()        │
  │                              │
  │  step: confirm-order         │
  │    ├─ DDB → CONFIRMED        │
  │    └─ emit OrderConfirmed ───────────────────────────────────────┐
  │                              │                                   │
  └──────────────────────────────┘                                   │
                                                                     │
               OrderConfirmed event                                  │
               matches Rule 3 + Rule 4 + Rule 5 + Rule 6            │
               ┌───────────────────┬───────────────────┐             │
               │                   │                   │             │
               v                   v                   v             v
        ┌────────────┐     ┌─────────────┐     ┌────────────┐  ┌─────────┐
        │billing-    │     │fulfillment- │     │notification│  │Firehose │
        │queue       │     │queue        │     │-queue      │  │→ S3     │
        └─────┬──────┘     └──────┬──────┘     └─────┬──────┘  └─────────┘
              │                   │                   │
              v                   v                   v
        ┌───────────┐     ┌─────────────┐     ┌────────────────┐
        │  BILLING  │     │ FULFILLMENT │     │  NOTIFICATION  │
        │  LAMBDA   │     │  LAMBDA     │     │  LAMBDA        │
        │           │     │             │     │                │
        │  Generate │     │  Initiate   │     │  Send confirm  │
        │  invoice  │     │  shipping   │     │  email (SES)   │
        └───────────┘     └─────────────┘     └────────────────┘


  ════════════════════════════════════════════════════════════════════════
  ORDER STATUS TIMELINE (DynamoDB)
  ════════════════════════════════════════════════════════════════════════

  PENDING ──> VALIDATING ──> RESERVED ──> CONFIRMED
     │            │              │             │
   intake      order svc      order svc     order svc
   lambda      step 1         step 2        step 4
```
