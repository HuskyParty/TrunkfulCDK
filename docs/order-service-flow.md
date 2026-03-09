# Order Service: Durable Saga + SQS DLQ Flow

## Full Message Flow

```
  EventBridge                    SQS                        Lambda
  (TrunkfulEventBus)       (order-queue)             (order-service)

 ┌──────────────┐      ┌───────────────────┐      ┌──────────────────────────┐
 │ OrderCreated │─────>│                   │─────>│  withDurableExecution()  │
 │   event      │ Rule │  order-queue      │ Poll │                          │
 └──────────────┘  #1  │                   │      │  ┌─ step: validate ────┐ │
                       │  visibilityTimeout│      │  │  update DDB status  │ │
                       │  = 60s            │      │  │  emit OrderValidated│ │
                       │                   │      │  └──────────┬─────────┘ │
                       │  maxReceiveCount  │      │       checkpoint        │
                       │  = 3              │      │             │            │
                       │                   │      │  ┌─ step: reserve ────┐ │
                       │                   │      │  │  update DDB status │ │
                       │                   │      │  │  emit OrderReserved│ │
                       │                   │      │  └──────────┬─────────┘ │
                       │                   │      │       checkpoint        │
                       │                   │      │             │            │
                       │                   │      │  ┌─ step: payment ────┐ │
                       │                   │      │  │  circuit breaker   │ │
                       │                   │      │  │  processPayment()  │ │
                       │                   │      │  └──────────┬─────────┘ │
                       │                   │      │       checkpoint        │
                       │                   │      │             │            │
                       │                   │      │  ┌─ step: confirm ────┐ │
                       │                   │      │  │  update CONFIRMED  │ │
                       │                   │      │  │  emit OrderConfirmed│ │
                       │                   │      │  └────────────────────┘ │
                       │                   │      │                          │
                       └───────────────────┘      └──────────────────────────┘
```

## Durable Execution Replay on Failure

```
  Lambda invocation starts
  │
  ├── context.step('validate-order')
  │   ├── Runs the function
  │   ├── CHECKPOINT ──> Lambda persists result to durable state store
  │   └── Returns { validated: true }
  │
  ├── context.step('reserve-inventory')
  │   ├── Runs the function
  │   ├── CHECKPOINT ──> persisted
  │   └── Returns { reserved: true }
  │
  ├── context.step('process-payment')   <── say this TIMES OUT mid-execution
  │   └── Lambda gets killed by 60s timeout
  │
  ╔═══════════════════════════════════════════════════════════╗
  ║  DURABLE REPLAY: Lambda is re-invoked automatically      ║
  ║                                                           ║
  ║  context.step('validate-order')                           ║
  ║    └── SKIPPED (reads checkpoint, already succeeded)      ║
  ║                                                           ║
  ║  context.step('reserve-inventory')                        ║
  ║    └── SKIPPED (reads checkpoint, already succeeded)      ║
  ║                                                           ║
  ║  context.step('process-payment')                          ║
  ║    └── RUNS AGAIN (no checkpoint = not completed)         ║
  ║    └── CHECKPOINT ──> persisted                           ║
  ║                                                           ║
  ║  context.step('confirm-order')                            ║
  ║    └── RUNS (new step)                                    ║
  ║    └── CHECKPOINT ──> persisted                           ║
  ║                                                           ║
  ║  Returns { orderId, status: 'CONFIRMED' }                 ║
  ╚═══════════════════════════════════════════════════════════╝
```

## SQS Retry + DLQ Escalation

```
                    ┌────────────────────────────────────────┐
                    │          SQS: order-queue               │
                    │                                         │
                    │  receiveCount tracks per-message        │
                    │  delivery attempts                      │
                    └──────────────┬──────────────────────────┘
                                   │
                        Lambda polls message
                                   │
             ┌─────────────────────┼─────────────────────┐
             │                     │                     │
        Attempt 1             Attempt 2             Attempt 3
             │                     │                     │
    ┌────────┴────────┐   ┌────────┴────────┐   ┌───────┴─────────┐
    │ Durable handler │   │ Durable handler │   │ Durable handler  │
    │ runs all steps  │   │ replays + runs  │   │ replays + runs   │
    │                 │   │ remaining steps │   │ remaining steps  │
    │  THROWS (e.g.   │   │                 │   │                  │
    │  payment svc    │   │  THROWS again   │   │  THROWS again    │
    │  is down)       │   │                 │   │                  │
    └─────────────────┘   └─────────────────┘   └────────┬─────────┘
                                                         │
                                              receiveCount = 3
                                              maxReceiveCount = 3
                                                         │
                                                         v
                                          ┌──────────────────────────┐
                                          │    SQS: order-dlq        │
                                          │    (Dead Letter Queue)   │
                                          │                          │
                                          │  Message parked here     │
                                          │  for investigation.      │
                                          │  14-day retention.       │
                                          │                          │
                                          │  CloudWatch alarm fires  │
                                          │  when messages > 0       │
                                          └──────────────────────────┘
```
