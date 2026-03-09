import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { SqsEventSource } from 'aws-cdk-lib/aws-lambda-event-sources';
import { Duration } from 'aws-cdk-lib';
import * as path from 'path';

interface ProcessingProps {
  stageName: string;
  reservedConcurrency: {
    orderService: number;
    inventoryService: number;
    billing: number;
    fulfillment: number;
    notification: number;
  };
  ordersTable: dynamodb.Table;
  inventoryTable: dynamodb.Table;
  idempotencyTable: dynamodb.Table;
  eventBus: events.EventBus;
  piiKey: kms.Key;
  paymentSecret: secretsmanager.Secret;
  orderQueue: sqs.Queue;
  inventoryQueue: sqs.Queue;
  billingQueue: sqs.Queue;
  fulfillmentQueue: sqs.Queue;
  notificationQueue: sqs.Queue;
}

export class ProcessingConstruct extends Construct {
  constructor(scope: Construct, id: string, props: ProcessingProps) {
    super(scope, id);

    // ---------------------------------------------------------------
    // 1. Order Service Lambda (Durable Execution)
    // Uses @aws/durable-execution-sdk-js withDurableExecution wrapper.
    // Durable execution mode enabled via Lambda console/CLI post-deploy.
    // ---------------------------------------------------------------
    const orderServiceFn = new NodejsFunction(this, 'OrderServiceFn', {
      functionName: `${props.stageName}-trunkful-order-service`,
      entry: path.join(__dirname, '../../lambda/order-service/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(60),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.orderService,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        INVENTORY_TABLE: props.inventoryTable.tableName,
        IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
      bundling: {
        // Bundle the durable execution SDK (not available in Lambda runtime)
        nodeModules: ['@aws/durable-execution-sdk-js'],
      },
    });

    props.ordersTable.grantReadWriteData(orderServiceFn);
    props.inventoryTable.grantReadWriteData(orderServiceFn);
    props.idempotencyTable.grantReadWriteData(orderServiceFn);
    props.eventBus.grantPutEventsTo(orderServiceFn);
    props.paymentSecret.grantRead(orderServiceFn);

    // Durable execution SDK needs permission to checkpoint and read state.
    // Use a scoped wildcard to avoid a circular dependency between function and role.
    orderServiceFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'lambda:GetDurableExecutionState',
          'lambda:CheckpointDurableExecution',
        ],
        resources: ['*'],
      }),
    );

    orderServiceFn.addEventSource(
      new SqsEventSource(props.orderQueue, { batchSize: 1 }),
    );

    // Version + alias for durable execution configuration
    const orderServiceVersion = orderServiceFn.currentVersion;
    new lambda.Alias(this, 'OrderServiceAlias', {
      aliasName: 'live',
      version: orderServiceVersion,
    });

    // ---------------------------------------------------------------
    // 2. Inventory Service Lambda (Durable Execution)
    // Uses @aws/durable-execution-sdk-js withDurableExecution wrapper.
    // Durable execution mode enabled via Lambda console/CLI post-deploy.
    // ---------------------------------------------------------------
    const inventoryServiceFn = new NodejsFunction(this, 'InventoryServiceFn', {
      functionName: `${props.stageName}-trunkful-inventory-service`,
      entry: path.join(__dirname, '../../lambda/inventory-service/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.inventoryService,
      environment: {
        INVENTORY_TABLE: props.inventoryTable.tableName,
        IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
      bundling: {
        nodeModules: ['@aws/durable-execution-sdk-js'],
      },
    });

    props.inventoryTable.grantReadWriteData(inventoryServiceFn);
    props.idempotencyTable.grantReadWriteData(inventoryServiceFn);
    props.eventBus.grantPutEventsTo(inventoryServiceFn);

    // Durable execution SDK needs permission to checkpoint and read state.
    inventoryServiceFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          'lambda:GetDurableExecutionState',
          'lambda:CheckpointDurableExecution',
        ],
        resources: ['*'],
      }),
    );

    inventoryServiceFn.addEventSource(
      new SqsEventSource(props.inventoryQueue, { batchSize: 1 }),
    );

    // Version + alias for durable execution configuration
    const inventoryServiceVersion = inventoryServiceFn.currentVersion;
    new lambda.Alias(this, 'InventoryServiceAlias', {
      aliasName: 'live',
      version: inventoryServiceVersion,
    });

    // ---------------------------------------------------------------
    // 3. Billing Lambda
    // ---------------------------------------------------------------
    const billingFn = new NodejsFunction(this, 'BillingFn', {
      functionName: `${props.stageName}-trunkful-billing`,
      entry: path.join(__dirname, '../../lambda/billing/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.billing,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
    });

    props.ordersTable.grantReadData(billingFn);
    props.eventBus.grantPutEventsTo(billingFn);

    billingFn.addEventSource(
      new SqsEventSource(props.billingQueue, { batchSize: 5 }),
    );

    // ---------------------------------------------------------------
    // 4. Fulfillment Lambda
    // ---------------------------------------------------------------
    const fulfillmentFn = new NodejsFunction(this, 'FulfillmentFn', {
      functionName: `${props.stageName}-trunkful-fulfillment`,
      entry: path.join(__dirname, '../../lambda/fulfillment/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.fulfillment,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
    });

    props.ordersTable.grantReadData(fulfillmentFn);
    props.eventBus.grantPutEventsTo(fulfillmentFn);

    fulfillmentFn.addEventSource(
      new SqsEventSource(props.fulfillmentQueue, { batchSize: 5 }),
    );

    // ---------------------------------------------------------------
    // 5. Notification Lambda
    // ---------------------------------------------------------------
    const notificationFn = new NodejsFunction(this, 'NotificationFn', {
      functionName: `${props.stageName}-trunkful-notification`,
      entry: path.join(__dirname, '../../lambda/notification/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.notification,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
        KMS_KEY_ID: props.piiKey.keyId,
      },
    });

    props.ordersTable.grantReadData(notificationFn);
    props.eventBus.grantPutEventsTo(notificationFn);
    props.piiKey.grantDecrypt(notificationFn);

    notificationFn.addEventSource(
      new SqsEventSource(props.notificationQueue, { batchSize: 5 }),
    );
  }
}
