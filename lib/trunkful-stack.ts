import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';

import { StageConfig, ALPHA_CONFIG } from './stage-config';
import { SecurityConstruct } from './constructs/security';
import { DataLayerConstruct } from './constructs/data-layer';
import { EventBusConstruct } from './constructs/event-bus';
import { QueuesConstruct } from './constructs/queues';
import { IngestionApiConstruct } from './constructs/ingestion-api';
import { IngestionIotConstruct } from './constructs/ingestion-iot';
import { IngestionS3Construct } from './constructs/ingestion-s3';
import { IngestionWarehouseConstruct } from './constructs/ingestion-warehouse';
import { ProcessingConstruct } from './constructs/processing';
import { AnalyticsConstruct } from './constructs/analytics';
import { MonitoringConstruct } from './constructs/monitoring';

export interface TrunkfulStackProps extends cdk.StackProps {
  stageConfig: StageConfig;
}

export class TrunkfulStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: TrunkfulStackProps) {
    super(scope, id, props);

    const config = props.stageConfig;
    const stageName = config.stageName;

    // ---------------------------------------------------------------
    // Phase 2: Foundation constructs
    // ---------------------------------------------------------------
    const security = new SecurityConstruct(this, 'Security', {
      stageName,
      removalPolicy: config.removalPolicy,
    });
    const dataLayer = new DataLayerConstruct(this, 'DataLayer', {
      stageName,
      removalPolicy: config.removalPolicy,
    });
    const eventBusConstruct = new EventBusConstruct(this, 'EventBus', {
      stageName,
    });
    const queues = new QueuesConstruct(this, 'Queues', {
      stageName,
    });

    // ---------------------------------------------------------------
    // Phase 5: Ingestion constructs
    // ---------------------------------------------------------------
    new IngestionApiConstruct(this, 'IngestionApi', {
      stageName,
      ordersTable: dataLayer.ordersTable,
      idempotencyTable: dataLayer.idempotencyTable,
      eventBus: eventBusConstruct.bus,
      piiKey: security.piiEncryptionKey,
    });

    new IngestionIotConstruct(this, 'IngestionIot', {
      stageName,
      ordersTable: dataLayer.ordersTable,
      idempotencyTable: dataLayer.idempotencyTable,
      eventBus: eventBusConstruct.bus,
    });

    new IngestionS3Construct(this, 'IngestionS3', {
      stageName,
      ordersTable: dataLayer.ordersTable,
      idempotencyTable: dataLayer.idempotencyTable,
      eventBus: eventBusConstruct.bus,
    });

    new IngestionWarehouseConstruct(this, 'IngestionWarehouse', {
      stageName,
      eventBus: eventBusConstruct.bus,
    });

    // ---------------------------------------------------------------
    // Processing construct
    // ---------------------------------------------------------------
    new ProcessingConstruct(this, 'Processing', {
      stageName,
      reservedConcurrency: config.reservedConcurrency,
      ordersTable: dataLayer.ordersTable,
      inventoryTable: dataLayer.inventoryTable,
      idempotencyTable: dataLayer.idempotencyTable,
      eventBus: eventBusConstruct.bus,
      piiKey: security.piiEncryptionKey,
      paymentSecret: security.paymentApiSecret,
      orderQueue: queues.orderQueue,
      inventoryQueue: queues.inventoryQueue,
      billingQueue: queues.billingQueue,
      fulfillmentQueue: queues.fulfillmentQueue,
      notificationQueue: queues.notificationQueue,
    });

    // ---------------------------------------------------------------
    // Analytics construct
    // ---------------------------------------------------------------
    const analytics = new AnalyticsConstruct(this, 'Analytics', {
      stageName,
      eventBus: eventBusConstruct.bus,
    });

    // ---------------------------------------------------------------
    // Monitoring construct
    // ---------------------------------------------------------------
    new MonitoringConstruct(this, 'Monitoring', {
      stageName,
      orderQueue: queues.orderQueue,
      inventoryQueue: queues.inventoryQueue,
      billingQueue: queues.billingQueue,
      fulfillmentQueue: queues.fulfillmentQueue,
      notificationQueue: queues.notificationQueue,
    });

    // ---------------------------------------------------------------
    // EventBridge Rules — route events from custom bus to SQS queues
    // ---------------------------------------------------------------
    const bus = eventBusConstruct.bus;

    // Rule 1: OrderCreated → Order Queue
    new events.Rule(this, 'OrderCreatedToOrderQueue', {
      eventBus: bus,
      ruleName: `${stageName}-OrderCreatedToOrderQueue`,
      eventPattern: {
        source: ['trunkful.orders'],
        detailType: ['OrderCreated'],
      },
      targets: [new targets.SqsQueue(queues.orderQueue)],
    });

    // Rule 2: OrderCreated + InventoryReceived + ReturnInitiated → Inventory Queue
    new events.Rule(this, 'InventoryEventsToInventoryQueue', {
      eventBus: bus,
      ruleName: `${stageName}-InventoryEventsToInventoryQueue`,
      eventPattern: {
        source: ['trunkful.orders'],
        detailType: [
          'OrderCreated',
          'InventoryReceived',
          'ReturnInitiated',
        ],
      },
      targets: [new targets.SqsQueue(queues.inventoryQueue)],
    });

    // Rule 3: OrderConfirmed → Billing Queue
    new events.Rule(this, 'OrderConfirmedToBillingQueue', {
      eventBus: bus,
      ruleName: `${stageName}-OrderConfirmedToBillingQueue`,
      eventPattern: {
        source: ['trunkful.orders'],
        detailType: ['OrderConfirmed'],
      },
      targets: [new targets.SqsQueue(queues.billingQueue)],
    });

    // Rule 4: OrderConfirmed → Fulfillment Queue
    new events.Rule(this, 'OrderConfirmedToFulfillmentQueue', {
      eventBus: bus,
      ruleName: `${stageName}-OrderConfirmedToFulfillmentQueue`,
      eventPattern: {
        source: ['trunkful.orders'],
        detailType: ['OrderConfirmed'],
      },
      targets: [new targets.SqsQueue(queues.fulfillmentQueue)],
    });

    // Rule 5: OrderConfirmed + OrderFailed + InventoryLow → Notification Queue
    new events.Rule(this, 'NotificationEvents', {
      eventBus: bus,
      ruleName: `${stageName}-NotificationEvents`,
      eventPattern: {
        source: ['trunkful.orders'],
        detailType: ['OrderConfirmed', 'OrderFailed', 'InventoryLow'],
      },
      targets: [new targets.SqsQueue(queues.notificationQueue)],
    });

    // Rule 6: All events → Firehose (analytics)
    const firehoseTargetRole = new iam.Role(this, 'FirehoseTargetRole', {
      assumedBy: new iam.ServicePrincipal('events.amazonaws.com'),
    });
    firehoseTargetRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'firehose:PutRecord',
          'firehose:PutRecordBatch',
        ],
        resources: [analytics.analyticsDeliveryStream.attrArn],
      }),
    );

    new events.Rule(this, 'AllEventsToFirehose', {
      eventBus: bus,
      ruleName: `${stageName}-AllEventsToFirehose`,
      eventPattern: {
        source: ['trunkful.orders'],
      },
      targets: [
        new targets.KinesisFirehoseStream(analytics.analyticsDeliveryStream, {
          message: events.RuleTargetInput.fromEventPath('$'),
        }),
      ],
    });
  }
}
