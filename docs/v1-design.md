# TrunkfulCDK — Event-Driven Inventory & Order Processing System

AWS CDK (TypeScript) implementation of an event-driven order processing architecture for a mid-size retailer with 5 ingestion channels.

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
│   │   ├── processing.ts                     # 5 processing Lambdas (Order/Inventory durable, Billing, Fulfillment, Notification)
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
│   │   ├── index.ts                          # Durable Lambda saga (validate → reserve → pay → confirm)
│   │   ├── circuit-breaker.ts                # DDB-backed circuit breaker
│   │   └── payment-client.ts                 # Payment provider stub
│   ├── inventory-service/index.ts            # Durable Lambda for inventory adjustments
│   ├── billing/index.ts                      # Billing stub
│   ├── fulfillment/index.ts                  # Fulfillment stub
│   └── notification/index.ts                 # SES email + SNS SMS + KMS decrypt
├── test/
│   └── trunkful-stack.test.ts                # Synth + resource assertions (17 tests)
├── docs/
│   └── cdk-getting-started.md                # CDK useful commands reference
├── package.json
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
- **Event Bus**: Custom EventBridge bus routes events to domain-specific SQS queues
- **Processing**: Order saga (validate → reserve → pay → confirm), Inventory adjustments, Billing, Fulfillment, Notifications
- **CQRS Read Path**: GET /orders/{orderId} uses API Gateway → DynamoDB direct integration (no Lambda)
- **Analytics**: Firehose → S3 data lake → Glue catalog → Athena queries
- **Security**: KMS CMK for PII encryption, Cognito auth, WAF with rate limiting + managed rule sets
- **Observability**: CloudWatch dashboard, DLQ alarms, X-Ray tracing on all Lambdas

## EventBridge Routing Rules

| Rule | Source Events | Target Queue |
|------|--------------|--------------|
| 1 | OrderCreated | Order Queue |
| 2 | OrderCreated, InventoryReceived, ReturnInitiated | Inventory Queue |
| 3 | OrderConfirmed | Billing Queue |
| 4 | OrderConfirmed | Fulfillment Queue |
| 5 | OrderConfirmed, OrderFailed, InventoryLow | Notification Queue |
| 6 | All events | Firehose (analytics) |
