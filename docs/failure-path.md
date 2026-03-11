# Failure Path: Payment Fails → Compensation → DLQ Escalation

## Scenario 1: Payment Declined (handled by Step Functions Catch)

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  ORDER SAGA (Step Functions)                                     │
  │                                                                  │
  │  ValidateOrder (Lambda)                                          │
  │    ├─ validate fields ─── OK                                     │
  │    ├─ DDB → VALIDATING                                           │
  │    └─ emit OrderValidated                                        │
  │         │                                                        │
  │         v                                                        │
  │  ReserveInventory (Lambda)                                       │
  │    ├─ DDB → RESERVED                                             │
  │    └─ emit OrderReserved                                         │
  │         │                                                        │
  │         v                                                        │
  │  ProcessPayment (Lambda)                                         │
  │    ├─ circuit breaker: CLOSED                                    │
  │    ├─ processPayment() ─── DECLINED                              │
  │    ├─ recordFailure('payment')                                   │
  │    └─ THROWS Error('Payment declined')                           │
  │         │                                                        │
  │         v  ──── Step Functions Catch fires ────                   │
  │                                                                  │
  │  ┌─────────────────────────────────────────────┐                 │
  │  │  CompensateRelease (Lambda)                  │                 │
  │  │    └─ emit InventoryReleaseRequested         │                 │
  │  └────────────────────┬────────────────────────┘                 │
  │                       │                                          │
  │                       v                                          │
  │  ┌─────────────────────────────────────────────┐                 │
  │  │  MarkFailedAfterComp (Lambda)                │                 │
  │  │    ├─ DDB → FAILED                           │                 │
  │  │    └─ emit OrderFailed ──────────────────────────────────┐    │
  │  └─────────────────────────────────────────────┘            │    │
  │                                                              │    │
  │  State machine execution completes (compensation ran).       │    │
  │  SQS message DELETED (trigger Lambda succeeded).             │    │
  └──────────────────────────────────────────────────────────────┘    │
                                                                      │
                OrderFailed event                                     │
                matches Rule 5: NotificationEvents→NotificationQueue        │
                      + Rule 6: AllEvents→Firehose                          │
                ┌────────────────────┐                                │
                │                    │                                │
                v                    v                                │
         ┌────────────┐      ┌───────────┐                           │
         │notification│      │ Firehose  │                           │
         │-queue      │      │ → S3      │                           │
         └─────┬──────┘      └───────────┘                           │
               │                                                      │
               v                                                      │
         ┌────────────────┐                                           │
         │  NOTIFICATION  │                                           │
         │  LAMBDA        │
         │                │
         │  "Sorry, your  │
         │   order could  │
         │   not be       │
         │   processed"   │
         └────────────────┘


  ORDER STATUS: PENDING ──> VALIDATING ──> RESERVED ──> FAILED
                                                          │
                                              compensation ran,
                                              inventory released,
                                              customer notified
```

## Scenario 2: Payment Service DOWN (circuit breaker + DLQ)

```
  ══════════════════════════════════════════════════════════════════
  TIMELINE: Payment service outage causes circuit breaker to OPEN
  ══════════════════════════════════════════════════════════════════

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
            Step Functions Catch → CompensateRelease → MarkFailed
  Order #7  checkCircuit() → OPEN → immediate throw, no call made
            (fail-fast with compensation)
  ...

            ── 30 seconds pass ──

  Order #N  checkCircuit() → HALF_OPEN → allows one test call
            payment call → success → recordSuccess() → CIRCUIT CLOSES
```

## Scenario 3: Trigger Lambda Crash → SQS Retry → DLQ

```
  ┌───────────────────────────────────────────────────────────────────────┐
  │  SQS: order-queue                                                     │
  │  ┌─────────────────────────────────────────┐                          │
  │  │  Message: Order #XYZ-999                 │                          │
  │  │  receiveCount: 0                         │                          │
  │  └─────────────────────────────────────────┘                          │
  └──────────────────────────┬────────────────────────────────────────────┘
                             │
      ═══════════════════════╪══════════════════════════════════════
      ATTEMPT 1              │         receiveCount: 1
      ═══════════════════════╪══════════════════════════════════════
                             v
         ┌──────────────────────────────────────────────────┐
         │  Trigger Lambda → StartExecution                  │
         │                                                   │
         │  State Machine runs:                              │
         │    ValidateOrder     → OK                         │
         │    ReserveInventory  → OK                         │
         │    ProcessPayment    → THROWS (service down)      │
         │    Catch → CompensateRelease → MarkFailed         │
         │                                                   │
         │  SM completes with compensation.                   │
         │  Trigger Lambda succeeds. SQS message DELETED.    │
         │  No SQS retry needed.                             │
         └──────────────────────────────────────────────────┘

  Note: If the trigger Lambda itself crashes (e.g. OOM before
  calling StartExecution), SQS retries up to 3 times, then DLQ.

      ═══════════════════════╪══════════════════════════════════════
      If trigger crashes:    │    receiveCount increments
      ═══════════════════════╪══════════════════════════════════════
                             │
               After 3 failed attempts:
                             │
                             v
         ┌──────────────────────────────────────────────────┐
         │  SQS: order-dlq  (Dead Letter Queue)              │
         │                                                   │
         │  Message parked. 14-day retention.                │
         │  CloudWatch alarm fires → engineer investigates.  │
         │  After fix: redrive from DLQ.                     │
         └──────────────────────────────────────────────────┘
```

**Key insight:** Step Functions handles saga failures **within a single
execution**. The Catch states run compensation inline -- there is no need
for SQS retries for business-level failures like payment decline. SQS
retries only fire if the trigger Lambda itself crashes before calling
StartExecution.

## Summary: Three Layers of Resilience

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
  │   │   ├─ Visual execution history for debugging             │   │
  │   │   └─ Handles INFRASTRUCTURE failures within execution   │   │
  │   │                                                         │   │
  │   │   ┌─────────────────────────────────────────────────┐   │   │
  │   │   │                                                 │   │   │
  │   │   │   Layer 3: SQS RETRY + DLQ                      │   │   │
  │   │   │   maxReceiveCount: 3 + dead letter queue         │   │   │
  │   │   │   ├─ Trigger Lambda crashes? → SQS retries 3x   │   │   │
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
