import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as synthetics from 'aws-cdk-lib/aws-synthetics';
import { Duration } from 'aws-cdk-lib';

interface MonitoringProps {
  stageName: string;
  orderQueue: sqs.Queue;
  inventoryQueue: sqs.Queue;
  billingQueue: sqs.Queue;
  fulfillmentQueue: sqs.Queue;
  notificationQueue: sqs.Queue;
  // Optional extended props for dashboard expansion
  orderSagaStateMachine?: sfn.StateMachine;
  inventoryWorkflowStateMachine?: sfn.StateMachine;
  lambdaFunctions?: lambda.IFunction[];
  dynamoTables?: dynamodb.Table[];
  // Placeholder API endpoint for canary health checks
  apiEndpointUrl?: string;
}

export class MonitoringConstruct extends Construct {
  constructor(scope: Construct, id: string, props: MonitoringProps) {
    super(scope, id);

    const queues: { name: string; queue: sqs.Queue }[] = [
      { name: 'Order', queue: props.orderQueue },
      { name: 'Inventory', queue: props.inventoryQueue },
      { name: 'Billing', queue: props.billingQueue },
      { name: 'Fulfillment', queue: props.fulfillmentQueue },
      { name: 'Notification', queue: props.notificationQueue },
    ];

    // ---------------------------------------------------------------
    // Row 1: Queue depth widgets for all 5 queues
    // ---------------------------------------------------------------
    const queueDepthWidgets = queues.map(
      ({ name, queue }) =>
        new cloudwatch.GraphWidget({
          title: `${name} Queue Depth`,
          left: [
            queue.metricApproximateNumberOfMessagesVisible({
              statistic: 'Maximum',
              period: Duration.minutes(1),
            }),
          ],
          width: 5,
        }),
    );

    // ---------------------------------------------------------------
    // Row 2: DLQ message count widgets for each queue's DLQ
    // ---------------------------------------------------------------
    const dlqWidgets = queues.map(({ name, queue }) => {
      const dlq = queue.deadLetterQueue!.queue;
      return new cloudwatch.GraphWidget({
        title: `${name} DLQ Messages`,
        left: [
          dlq.metricApproximateNumberOfMessagesVisible({
            statistic: 'Maximum',
            period: Duration.minutes(1),
          }),
        ],
        width: 5,
      });
    });

    // ---------------------------------------------------------------
    // Row 3: SQS queue age widgets for all 5 queues
    // ---------------------------------------------------------------
    const queueAgeWidgets = queues.map(
      ({ name, queue }) =>
        new cloudwatch.GraphWidget({
          title: `${name} Queue Age of Oldest Message (s)`,
          left: [
            queue.metricApproximateAgeOfOldestMessage({
              statistic: 'Maximum',
              period: Duration.minutes(1),
            }),
          ],
          width: 5,
        }),
    );

    // ---------------------------------------------------------------
    // Row 4: Step Functions health widgets
    // ---------------------------------------------------------------
    const stepFunctionsWidgets: cloudwatch.GraphWidget[] = [];
    const stateMachines: { name: string; sm: sfn.StateMachine }[] = [];
    if (props.orderSagaStateMachine) {
      stateMachines.push({ name: 'OrderSaga', sm: props.orderSagaStateMachine });
    }
    if (props.inventoryWorkflowStateMachine) {
      stateMachines.push({ name: 'InventoryWorkflow', sm: props.inventoryWorkflowStateMachine });
    }
    for (const { name, sm } of stateMachines) {
      stepFunctionsWidgets.push(
        new cloudwatch.GraphWidget({
          title: `${name} Step Functions Health`,
          left: [
            sm.metricStarted({ statistic: 'Sum', period: Duration.minutes(5) }),
            sm.metricSucceeded({ statistic: 'Sum', period: Duration.minutes(5) }),
            sm.metricFailed({ statistic: 'Sum', period: Duration.minutes(5) }),
            sm.metricTimedOut({ statistic: 'Sum', period: Duration.minutes(5) }),
          ],
          width: 12,
        }),
      );
    }

    // ---------------------------------------------------------------
    // Row 5: Lambda health widgets (Errors, Duration, ConcurrentExecutions)
    // ---------------------------------------------------------------
    const lambdaWidgets: cloudwatch.GraphWidget[] = [];
    for (const fn of props.lambdaFunctions ?? []) {
      lambdaWidgets.push(
        new cloudwatch.GraphWidget({
          title: `Lambda: ${fn.functionName}`,
          left: [
            fn.metricErrors({ statistic: 'Sum', period: Duration.minutes(5) }),
            fn.metricDuration({ statistic: 'Average', period: Duration.minutes(5) }),
          ],
          right: [
            new cloudwatch.Metric({
              namespace: 'AWS/Lambda',
              metricName: 'ConcurrentExecutions',
              dimensionsMap: { FunctionName: fn.functionName },
              statistic: 'Maximum',
              period: Duration.minutes(5),
            }),
          ],
          width: 8,
        }),
      );
    }

    // ---------------------------------------------------------------
    // Row 6: DynamoDB health widgets (ThrottledRequests, SuccessfulRequestLatency)
    // ---------------------------------------------------------------
    const dynamoWidgets: cloudwatch.GraphWidget[] = [];
    for (const table of props.dynamoTables ?? []) {
      dynamoWidgets.push(
        new cloudwatch.GraphWidget({
          title: `DynamoDB: ${table.tableName}`,
          left: [
            table.metricThrottledRequestsForOperations({
              operations: [
                dynamodb.Operation.GET_ITEM,
                dynamodb.Operation.PUT_ITEM,
                dynamodb.Operation.UPDATE_ITEM,
                dynamodb.Operation.DELETE_ITEM,
                dynamodb.Operation.QUERY,
              ],
              statistic: 'Sum',
              period: Duration.minutes(5),
            }),
          ],
          right: [
            table.metricSuccessfulRequestLatency({
              dimensionsMap: { TableName: table.tableName, Operation: 'Query' },
              statistic: 'Average',
              period: Duration.minutes(5),
            }),
          ],
          width: 8,
        }),
      );
    }

    // ---------------------------------------------------------------
    // Assemble dashboard rows (only include non-empty rows)
    // ---------------------------------------------------------------
    const dashboardRows: cloudwatch.IWidget[][] = [
      queueDepthWidgets,
      dlqWidgets,
      queueAgeWidgets,
    ];
    if (stepFunctionsWidgets.length > 0) {
      dashboardRows.push(stepFunctionsWidgets);
    }
    if (lambdaWidgets.length > 0) {
      dashboardRows.push(lambdaWidgets);
    }
    if (dynamoWidgets.length > 0) {
      dashboardRows.push(dynamoWidgets);
    }

    // ---------------------------------------------------------------
    // CloudWatch Dashboard
    // ---------------------------------------------------------------
    new cloudwatch.Dashboard(this, 'TrunkfulDashboard', {
      dashboardName: `${props.stageName}-TrunkfulDashboard`,
      widgets: dashboardRows,
    });

    // ---------------------------------------------------------------
    // Alarms: DLQ messages > 0 for each queue's DLQ
    // ---------------------------------------------------------------
    queues.forEach(({ name, queue }) => {
      const dlq = queue.deadLetterQueue!.queue;

      new cloudwatch.Alarm(this, `${name}DlqAlarm`, {
        alarmName: `${props.stageName}-Trunkful-${name}-DLQ-Messages`,
        alarmDescription:
          `Messages appeared in the ${name} dead-letter queue. ` +
          `See runbook: https://notion.so/team/runbook-dlq-nonempty`,
        metric: dlq.metricApproximateNumberOfMessagesVisible({
          statistic: 'Maximum',
          period: Duration.seconds(300),
        }),
        evaluationPeriods: 1,
        threshold: 0,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      });
    });

    // ---------------------------------------------------------------
    // Alarms: Queue age of oldest message > 5 minutes for ALL 5 queues
    // ---------------------------------------------------------------
    queues.forEach(({ name, queue }) => {
      new cloudwatch.Alarm(this, `${name}QueueAgeAlarm`, {
        alarmName: `${props.stageName}-Trunkful-${name}-Queue-Age`,
        alarmDescription:
          `Oldest message in the ${name} queue is older than 5 minutes. ` +
          `See runbook: https://notion.so/team/runbook-queue-age`,
        metric: queue.metricApproximateAgeOfOldestMessage({
          statistic: 'Maximum',
          period: Duration.seconds(60),
        }),
        evaluationPeriods: 2,
        threshold: 300,
        comparisonOperator:
          cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      });
    });

    // ---------------------------------------------------------------
    // Alarm (P1): High failure rate — OrdersFailed / (OrdersConfirmed + OrdersFailed) > 5%
    // ---------------------------------------------------------------
    const ordersFailedMetric = new cloudwatch.Metric({
      namespace: 'RetailPlatform',
      metricName: 'OrdersFailed',
      statistic: 'Sum',
      period: Duration.minutes(5),
    });

    const ordersConfirmedMetric = new cloudwatch.Metric({
      namespace: 'RetailPlatform',
      metricName: 'OrdersConfirmed',
      statistic: 'Sum',
      period: Duration.minutes(5),
    });

    const failureRateExpression = new cloudwatch.MathExpression({
      expression: 'failed / (confirmed + failed)',
      usingMetrics: {
        failed: ordersFailedMetric,
        confirmed: ordersConfirmedMetric,
      },
      period: Duration.minutes(5),
      label: 'Order Failure Rate',
    });

    new cloudwatch.Alarm(this, 'HighOrderFailureRateAlarm', {
      alarmName: `${props.stageName}-Trunkful-High-Order-Failure-Rate`,
      alarmDescription:
        'P1: Order failure rate exceeded 5% over the last 5 minutes ' +
        '(OrdersFailed / (OrdersConfirmed + OrdersFailed) > 0.05). ' +
        'See runbook: https://notion.so/team/runbook-high-failure-rate',
      metric: failureRateExpression,
      evaluationPeriods: 1,
      threshold: 0.05,
      comparisonOperator:
        cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
      treatMissingData: cloudwatch.TreatMissingData.NOT_BREACHING,
    });

    // ---------------------------------------------------------------
    // CloudWatch Synthetics Canaries
    // ---------------------------------------------------------------

    // Canary 1: Order Flow — runs every 5 minutes
    new synthetics.Canary(this, 'OrderFlowCanary', {
      canaryName: `${props.stageName}-order-flow`,
      schedule: synthetics.Schedule.rate(Duration.minutes(5)),
      runtime: synthetics.Runtime.SYNTHETICS_NODEJS_PUPPETEER_9_1,
      test: synthetics.Test.custom({
        code: synthetics.Code.fromInline(`
const synthetics = require('Synthetics');
const log = require('SyntheticsLogger');

const apiUrl = process.env.API_ENDPOINT_URL || 'https://placeholder.execute-api.us-east-1.amazonaws.com/prod';

exports.handler = async () => {
  log.info('OrderFlowCanary: starting order flow health check');

  // Step 1: POST /orders — place a synthetic order
  const orderPayload = JSON.stringify({
    customerId: 'canary-customer-001',
    items: [{ sku: 'SKU-CANARY-001', quantity: 1 }],
  });

  const requestOptionsPost = {
    hostname: new URL(apiUrl).hostname,
    path: new URL(apiUrl).pathname.replace(/\\/$/, '') + '/orders',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(orderPayload),
      'x-canary': 'true',
    },
    body: orderPayload,
  };

  const response = await synthetics.executeHttpStep(
    'PlaceOrder',
    requestOptionsPost,
    (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        throw new Error('PlaceOrder returned non-2xx: ' + res.statusCode);
      }
    },
  );

  log.info('OrderFlowCanary: order placed, response status ' + (response && response.statusCode));
};
        `),
        handler: 'index.handler',
      }),
      environmentVariables: {
        API_ENDPOINT_URL: props.apiEndpointUrl ?? 'https://placeholder.execute-api.us-east-1.amazonaws.com/prod',
      },
    });

    // Canary 2: API Health — runs every 1 minute
    new synthetics.Canary(this, 'ApiHealthCanary', {
      canaryName: `${props.stageName}-api-health`,
      schedule: synthetics.Schedule.rate(Duration.minutes(1)),
      runtime: synthetics.Runtime.SYNTHETICS_NODEJS_PUPPETEER_9_1,
      test: synthetics.Test.custom({
        code: synthetics.Code.fromInline(`
const synthetics = require('Synthetics');
const log = require('SyntheticsLogger');

const apiUrl = process.env.API_ENDPOINT_URL || 'https://placeholder.execute-api.us-east-1.amazonaws.com/prod';

exports.handler = async () => {
  log.info('ApiHealthCanary: starting API health check');

  const parsedUrl = new URL(apiUrl);

  const requestOptions = {
    hostname: parsedUrl.hostname,
    path: parsedUrl.pathname.replace(/\\/$/, '') + '/health',
    method: 'GET',
    headers: {
      'x-canary': 'true',
    },
  };

  await synthetics.executeHttpStep(
    'CheckHealth',
    requestOptions,
    (res) => {
      if (res.statusCode < 200 || res.statusCode >= 300) {
        throw new Error('Health check returned non-2xx: ' + res.statusCode);
      }
    },
  );

  log.info('ApiHealthCanary: health check passed');
};
        `),
        handler: 'index.handler',
      }),
      environmentVariables: {
        API_ENDPOINT_URL: props.apiEndpointUrl ?? 'https://placeholder.execute-api.us-east-1.amazonaws.com/prod',
      },
    });
  }
}
