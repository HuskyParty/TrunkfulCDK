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
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
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

    const lambdaDir = path.join(__dirname, '../../lambda');

    // Helper: create a lightweight step function lambda
    const createStepFn = (
      constructId: string,
      entry: string,
      env: Record<string, string>,
      timeout = 10,
    ): NodejsFunction =>
      new NodejsFunction(this, constructId, {
        functionName: `${props.stageName}-trunkful-${constructId.replace(/([A-Z])/g, '-$1').toLowerCase().replace(/^-/, '')}`,
        entry: path.join(lambdaDir, entry),
        handler: 'handler',
        runtime: lambda.Runtime.NODEJS_22_X,
        timeout: Duration.seconds(timeout),
        memorySize: 128,
        tracing: lambda.Tracing.ACTIVE,
        environment: env,
      });

    // =================================================================
    // 1. Order Saga — Step Lambdas
    // =================================================================

    const orderEnv = {
      ORDERS_TABLE: props.ordersTable.tableName,
      EVENT_BUS_NAME: props.eventBus.eventBusName,
    };

    const validateFn = createStepFn('OrderValidate', 'order-steps/validate.ts', orderEnv);
    const reserveInventoryFn = createStepFn('OrderReserve', 'order-steps/reserve-inventory.ts', {
      ...orderEnv,
      INVENTORY_TABLE: props.inventoryTable.tableName,
    });
    const processPaymentFn = createStepFn('OrderPayment', 'order-steps/process-payment.ts', {
      ORDERS_TABLE: props.ordersTable.tableName,
      PAYMENT_SECRET_ARN: props.paymentSecret.secretArn,
      EVENT_BUS_NAME: props.eventBus.eventBusName,
    }, 15);
    const confirmOrderFn = createStepFn('OrderConfirm', 'order-steps/confirm-order.ts', orderEnv);
    const releaseInventoryFn = createStepFn('OrderRelease', 'order-steps/release-inventory.ts', {
      EVENT_BUS_NAME: props.eventBus.eventBusName,
      INVENTORY_TABLE: props.inventoryTable.tableName,
    });
    const markFailedFn = createStepFn('OrderMarkFailed', 'order-steps/mark-failed.ts', orderEnv);
    const emitEventFn = createStepFn('OrderEmitEvent', 'order-steps/emit-event.ts', {
      EVENT_BUS_NAME: props.eventBus.eventBusName,
    });

    // Permissions for order step lambdas — scoped to minimum required actions
    for (const fn of [validateFn, confirmOrderFn, markFailedFn]) {
      props.ordersTable.grant(fn, 'dynamodb:GetItem', 'dynamodb:UpdateItem');
      props.eventBus.grantPutEventsTo(fn);
    }

    // reserveInventoryFn needs GetItem/UpdateItem on both Orders and Inventory tables
    props.ordersTable.grant(reserveInventoryFn, 'dynamodb:GetItem', 'dynamodb:UpdateItem');
    props.inventoryTable.grant(reserveInventoryFn, 'dynamodb:GetItem', 'dynamodb:UpdateItem');
    props.eventBus.grantPutEventsTo(reserveInventoryFn);

    props.ordersTable.grant(processPaymentFn, 'dynamodb:GetItem', 'dynamodb:UpdateItem'); // circuit breaker state
    props.paymentSecret.grantRead(processPaymentFn);
    props.eventBus.grantPutEventsTo(processPaymentFn);
    props.inventoryTable.grant(releaseInventoryFn, 'dynamodb:GetItem', 'dynamodb:UpdateItem');
    props.eventBus.grantPutEventsTo(releaseInventoryFn);
    // emitEventFn only needs to put events on the bus
    props.eventBus.grantPutEventsTo(emitEventFn);

    // =================================================================
    // 2. Order Saga — Step Functions State Machine
    // =================================================================

    // Error handlers
    const markFailedEarly = new tasks.LambdaInvoke(this, 'MarkFailedEarly', {
      lambdaFunction: markFailedFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const emitOrderFailedEarly = new tasks.LambdaInvoke(this, 'EmitOrderFailedEarly', {
      lambdaFunction: emitEventFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
      payload: sfn.TaskInput.fromObject({
        'eventType': 'OrderFailed',
        'orderId.$': '$.orderId',
        'reason.$': '$.error.Cause',
      }),
    });

    // Early failure path: mark failed then emit event
    markFailedEarly.next(emitOrderFailedEarly);

    const compensateRelease = new tasks.LambdaInvoke(this, 'CompensateRelease', {
      lambdaFunction: releaseInventoryFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const markFailedAfterComp = new tasks.LambdaInvoke(this, 'MarkFailedAfterComp', {
      lambdaFunction: markFailedFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const emitOrderFailed = new tasks.LambdaInvoke(this, 'EmitOrderFailed', {
      lambdaFunction: emitEventFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
      payload: sfn.TaskInput.fromObject({
        'eventType': 'OrderFailed',
        'orderId.$': '$.orderId',
        'reason.$': '$.error.Cause',
      }),
    });

    // Compensated failure path: release inventory → mark failed → emit event
    compensateRelease.next(markFailedAfterComp).next(emitOrderFailed);

    // Main saga steps
    const validateOrder = new tasks.LambdaInvoke(this, 'ValidateOrder', {
      lambdaFunction: validateFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const reserveInventory = new tasks.LambdaInvoke(this, 'ReserveInventory', {
      lambdaFunction: reserveInventoryFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const processPayment = new tasks.LambdaInvoke(this, 'ProcessPayment', {
      lambdaFunction: processPaymentFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const confirmOrder = new tasks.LambdaInvoke(this, 'ConfirmOrder', {
      lambdaFunction: confirmOrderFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
    });

    const emitOrderConfirmed = new tasks.LambdaInvoke(this, 'EmitOrderConfirmed', {
      lambdaFunction: emitEventFn,
      payloadResponseOnly: true,
      resultPath: sfn.JsonPath.DISCARD,
      payload: sfn.TaskInput.fromObject({
        'eventType': 'OrderConfirmed',
        'orderId.$': '$.orderId',
      }),
    });

    // Catch: before inventory reserved → mark failed then emit
    validateOrder.addCatch(markFailedEarly, { resultPath: '$.error' });
    reserveInventory.addCatch(markFailedEarly, { resultPath: '$.error' });

    // Catch: after inventory reserved → release then mark failed then emit
    processPayment.addCatch(compensateRelease, { resultPath: '$.error' });
    confirmOrder.addCatch(compensateRelease, { resultPath: '$.error' });

    // Per-state retry configuration for transient errors
    validateOrder.addRetry({
      errors: ['States.TaskFailed'],
      maxAttempts: 1,
      interval: Duration.seconds(1),
      backoffRate: 2,
    });
    reserveInventory.addRetry({
      errors: ['States.TaskFailed'],
      maxAttempts: 2,
      interval: Duration.seconds(1),
      backoffRate: 2,
    });
    processPayment.addRetry({
      errors: ['States.TaskFailed'],
      maxAttempts: 3,
      interval: Duration.seconds(2),
      backoffRate: 3,
    });
    confirmOrder.addRetry({
      errors: ['States.TaskFailed'],
      maxAttempts: 2,
      interval: Duration.seconds(1),
      backoffRate: 2,
    });

    const orderSagaChain = validateOrder
      .next(reserveInventory)
      .next(processPayment)
      .next(confirmOrder)
      .next(emitOrderConfirmed);

    const orderSagaSM = new sfn.StateMachine(this, 'OrderSagaSM', {
      stateMachineName: `${props.stageName}-trunkful-order-saga`,
      definitionBody: sfn.DefinitionBody.fromChainable(orderSagaChain),
      timeout: Duration.minutes(5),
      tracingEnabled: true,
    });

    // =================================================================
    // 3. Order Saga — Starter Lambda (SQS → Step Functions)
    // =================================================================

    const orderSagaTriggerFn = new NodejsFunction(this, 'OrderSagaTrigger', {
      functionName: `${props.stageName}-trunkful-order-saga-trigger`,
      entry: path.join(lambdaDir, 'order-steps/trigger.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(10),
      memorySize: 128,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.orderService,
      environment: {
        STATE_MACHINE_ARN: orderSagaSM.stateMachineArn,
      },
    });

    orderSagaSM.grantStartExecution(orderSagaTriggerFn);

    orderSagaTriggerFn.addEventSource(
      new SqsEventSource(props.orderQueue, { batchSize: 1 }),
    );

    // =================================================================
    // 4. Inventory Workflow — Step Lambda
    // =================================================================

    const processItemFn = createStepFn('InvProcessItem', 'inventory-steps/process-item.ts', {
      INVENTORY_TABLE: props.inventoryTable.tableName,
      EVENT_BUS_NAME: props.eventBus.eventBusName,
      IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
    });

    props.inventoryTable.grantReadWriteData(processItemFn);
    props.idempotencyTable.grantReadWriteData(processItemFn);
    props.eventBus.grantPutEventsTo(processItemFn);

    // =================================================================
    // 5. Inventory Workflow — Step Functions State Machine
    // =================================================================

    // Normalize EventBridge envelope (detail-type has a hyphen)
    const normalizeInput = new sfn.Pass(this, 'NormalizeInventoryInput', {
      parameters: {
        'detailType.$': "$['detail-type']",
        'detail.$': '$.detail',
      },
    });

    // OrderCreated branch: Map over items
    const processReservationItem = new tasks.LambdaInvoke(this, 'ProcessReservationItem', {
      lambdaFunction: processItemFn,
      payloadResponseOnly: true,
    });

    const mapOrderItems = new sfn.Map(this, 'MapOrderItems', {
      itemsPath: '$.detail.items',
      itemSelector: {
        'sku.$': '$$.Map.Item.Value.sku',
        'quantity.$': '$$.Map.Item.Value.quantity',
        'orderId.$': '$.detail.orderId',
        'isReservation': true,
      },
      resultPath: sfn.JsonPath.DISCARD,
    });
    mapOrderItems.itemProcessor(processReservationItem);

    // Generic branch: pass detail directly to process-item
    const extractGenericDetail = new sfn.Pass(this, 'ExtractGenericDetail', {
      inputPath: '$.detail',
    });

    const processGenericItem = new tasks.LambdaInvoke(this, 'ProcessGenericItem', {
      lambdaFunction: processItemFn,
      payloadResponseOnly: true,
    });

    const eventTypeChoice = new sfn.Choice(this, 'InventoryEventType')
      .when(
        sfn.Condition.stringEquals('$.detailType', 'OrderCreated'),
        mapOrderItems,
      )
      .otherwise(extractGenericDetail.next(processGenericItem));

    const inventoryDefinition = normalizeInput.next(eventTypeChoice);

    const inventoryWorkflowSM = new sfn.StateMachine(this, 'InventoryWorkflowSM', {
      stateMachineName: `${props.stageName}-trunkful-inventory-workflow`,
      definitionBody: sfn.DefinitionBody.fromChainable(inventoryDefinition),
      timeout: Duration.minutes(5),
      tracingEnabled: true,
    });

    // =================================================================
    // 6. Inventory Workflow — Starter Lambda (SQS → Step Functions)
    // =================================================================

    const inventoryWorkflowTriggerFn = new NodejsFunction(this, 'InventoryWorkflowTrigger', {
      functionName: `${props.stageName}-trunkful-inventory-workflow-trigger`,
      entry: path.join(lambdaDir, 'inventory-steps/trigger.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(10),
      memorySize: 128,
      tracing: lambda.Tracing.ACTIVE,
      reservedConcurrentExecutions: props.reservedConcurrency.inventoryService,
      environment: {
        STATE_MACHINE_ARN: inventoryWorkflowSM.stateMachineArn,
      },
    });

    inventoryWorkflowSM.grantStartExecution(inventoryWorkflowTriggerFn);

    inventoryWorkflowTriggerFn.addEventSource(
      new SqsEventSource(props.inventoryQueue, { batchSize: 1 }),
    );

    // =================================================================
    // 7. Billing Lambda (unchanged)
    // =================================================================
    const billingFn = new NodejsFunction(this, 'BillingFn', {
      functionName: `${props.stageName}-trunkful-billing`,
      entry: path.join(lambdaDir, 'billing/index.ts'),
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

    // =================================================================
    // 8. Fulfillment Lambda (unchanged)
    // =================================================================
    const fulfillmentFn = new NodejsFunction(this, 'FulfillmentFn', {
      functionName: `${props.stageName}-trunkful-fulfillment`,
      entry: path.join(lambdaDir, 'fulfillment/index.ts'),
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

    // =================================================================
    // 9. Notification Lambda (unchanged)
    // =================================================================
    const notificationFn = new NodejsFunction(this, 'NotificationFn', {
      functionName: `${props.stageName}-trunkful-notification`,
      entry: path.join(lambdaDir, 'notification/index.ts'),
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
    notificationFn.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendEmail', 'sns:Publish', 'cloudwatch:PutMetricData'],
      resources: ['*'],
    }));

    notificationFn.addEventSource(
      new SqsEventSource(props.notificationQueue, { batchSize: 5 }),
    );
  }
}
