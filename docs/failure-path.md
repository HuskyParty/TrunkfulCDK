# Failure Path: Payment Fails → Compensation → DLQ Escalation

## Scenario 1: Payment Declined (handled gracefully)

```
  ┌──────────────────────────────────────────────────────────────────┐
  │  ORDER SERVICE (durable)                                         │
  │                                                                  │
  │  step: validate-order                                            │
  │    ├─ validate fields ─── OK                                     │
  │    ├─ DDB → VALIDATING                                           │
  │    ├─ emit OrderValidated                                        │
  │    └─ CHECKPOINT ✓                                               │
  │                                                                  │
  │  step: reserve-inventory                                         │
  │    ├─ DDB → RESERVED                                             │
  │    ├─ emit OrderReserved                                         │
  │    └─ CHECKPOINT ✓                                               │
  │                                                                  │
  │  step: process-payment                                           │
  │    ├─ circuit breaker: CLOSED                                    │
  │    ├─ processPayment() ─── DECLINED                              │
  │    ├─ recordFailure('payment')                                   │
  │    └─ THROWS Error('Payment declined')                           │
  │         │                                                        │
  │         v                                                        │
  │  ┌─────────────────────────────────────────────┐                 │
  │  │  COMPENSATION (try/catch in handler)         │                 │
  │  │                                              │                 │
  │  │  1. emit InventoryReleaseRequested           │                 │
  │  │     └─ inventory service releases hold       │                 │
  │  │                                              │                 │
  │  │  2. DDB → FAILED                             │                 │
  │  │                                              │                 │
  │  │  3. emit OrderFailed ──────────────────────────────────────┐  │
  │  │     └─ reason: "Payment declined"            │             │  │
  │  └─────────────────────────────────────────────┘             │  │
  │                                                               │  │
  │  return { orderId, status: 'FAILED' }  ← handler succeeds    │  │
  │  SQS message DELETED (no retry needed)                        │  │
  └───────────────────────────────────────────────────────────────┘  │
                                                                     │
                OrderFailed event                                    │
                matches Rule 5 + Rule 6                              │
                ┌────────────────────┐                               │
                │                    │                               │
                v                    v                               │
         ┌────────────┐      ┌───────────┐                          │
         │notification│      │ Firehose  │                          │
         │-queue      │      │ → S3      │                          │
         └─────┬──────┘      └───────────┘                          │
               │                                                     │
               v                                                     │
         ┌────────────────┐                                          │
         │  NOTIFICATION  │                                          │
         │  LAMBDA        │                                          │
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
  Order #7  checkCircuit() → OPEN → immediate throw, no call made
  ...       (all orders fail-fast with compensation)

            ── 30 seconds pass ──

  Order #N  checkCircuit() → HALF_OPEN → allows one test call
            payment call → success → recordSuccess() → CIRCUIT CLOSES
```

## Scenario 3: Unhandled Crash → SQS Retry → DLQ

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
         │  ORDER SERVICE (durable)                          │
         │                                                   │
         │  step: validate-order     → CHECKPOINT ✓          │
         │  step: reserve-inventory  → CHECKPOINT ✓          │
         │  step: process-payment    → Lambda OOM KILLED     │
         │                              (128MB not enough)   │
         │                                                   │
         │  ╔═════════════════════════════════════════════╗   │
         │  ║  UNHANDLED: Lambda runtime killed process   ║   │
         │  ║  No catch block reached                     ║   │
         │  ║  SQS never gets deleteMessage               ║   │
         │  ║  visibilityTimeout expires (60s)            ║   │
         │  ╚═════════════════════════════════════════════╝   │
         └──────────────────────────────────────────────────┘

      ═══════════════════════╪══════════════════════════════════════
      ATTEMPT 2              │         receiveCount: 2
      ═══════════════════════╪══════════════════════════════════════
                             v
         ┌──────────────────────────────────────────────────┐
         │  ORDER SERVICE (durable) — REPLAY                 │
         │                                                   │
         │  step: validate-order     → SKIP (checkpoint hit) │
         │  step: reserve-inventory  → SKIP (checkpoint hit) │
         │  step: process-payment    → Lambda OOM KILLED     │
         │                              (same bug)           │
         └──────────────────────────────────────────────────┘

      ═══════════════════════╪══════════════════════════════════════
      ATTEMPT 3 (final)      │         receiveCount: 3
      ═══════════════════════╪══════════════════════════════════════
                             v
         ┌──────────────────────────────────────────────────┐
         │  ORDER SERVICE (durable) — REPLAY                 │
         │                                                   │
         │  step: validate-order     → SKIP (checkpoint hit) │
         │  step: reserve-inventory  → SKIP (checkpoint hit) │
         │  step: process-payment    → Lambda OOM KILLED     │
         │                              (same bug)           │
         └──────────────────────────────────────────────────┘

      ═══════════════════════╪══════════════════════════════════════
      receiveCount (3) >=    │    maxReceiveCount (3)
      ═══════════════════════╪══════════════════════════════════════
                             │
                             v
         ┌──────────────────────────────────────────────────┐
         │  SQS: order-dlq  (Dead Letter Queue)              │
         │                                                   │
         │  ┌─────────────────────────────────────────────┐  │
         │  │  Message: Order #XYZ-999                     │  │
         │  │  Original queue: order-queue                 │  │
         │  │  receiveCount at death: 3                    │  │
         │  │  First received: 2026-03-09T10:00:00Z        │  │
         │  │  Retention: 14 days                          │  │
         │  └─────────────────────────────────────────────┘  │
         │                                                   │
         └──────────────────────────┬────────────────────────┘
                                    │
                          ┌─────────┘
                          v
         ┌──────────────────────────────────────────────────┐
         │  CloudWatch Alarm: OrderDlqAlarm                  │
         │                                                   │
         │  ApproximateNumberOfMessagesVisible > 0           │
         │  ──────────────────────────────────               │
         │  ALARM STATE                                      │
         │                                                   │
         │  → SNS notification to on-call engineer           │
         │  → Dashboard widget turns red                     │
         │  → Engineer inspects DLQ message, finds OOM       │
         │  → Increases Lambda memory, redrives message      │
         └──────────────────────────────────────────────────┘


  ════════════════════════════════════════════════════════════════════
  KEY INSIGHT: Durable checkpoints survived all 3 attempts.
  After the engineer fixes the OOM bug and redrives from DLQ,
  the Lambda replays and SKIPS validate + reserve (already done).
  Only the payment step runs, saving time and preventing
  double-reservation.
  ════════════════════════════════════════════════════════════════════
```

## Summary: Three Layers of Resilience

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
