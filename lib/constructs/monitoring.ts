import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { Duration } from 'aws-cdk-lib';

interface MonitoringProps {
  stageName: string;
  orderQueue: sqs.Queue;
  inventoryQueue: sqs.Queue;
  billingQueue: sqs.Queue;
  fulfillmentQueue: sqs.Queue;
  notificationQueue: sqs.Queue;
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
    // CloudWatch Dashboard
    // ---------------------------------------------------------------
    new cloudwatch.Dashboard(this, 'TrunkfulDashboard', {
      dashboardName: `${props.stageName}-TrunkfulDashboard`,
      widgets: [
        queueDepthWidgets,
        dlqWidgets,
      ],
    });

    // ---------------------------------------------------------------
    // Alarms: DLQ messages > 0 for each queue's DLQ
    // ---------------------------------------------------------------
    queues.forEach(({ name, queue }) => {
      const dlq = queue.deadLetterQueue!.queue;

      new cloudwatch.Alarm(this, `${name}DlqAlarm`, {
        alarmName: `${props.stageName}-Trunkful-${name}-DLQ-Messages`,
        alarmDescription: `Messages appeared in the ${name} dead-letter queue`,
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
    // Alarm: Order queue age of oldest message > 5 minutes
    // ---------------------------------------------------------------
    new cloudwatch.Alarm(this, 'OrderQueueAgeAlarm', {
      alarmName: `${props.stageName}-Trunkful-Order-Queue-Age`,
      alarmDescription:
        'Oldest message in the order queue is older than 5 minutes',
      metric: props.orderQueue.metricApproximateAgeOfOldestMessage({
        statistic: 'Maximum',
        period: Duration.seconds(60),
      }),
      evaluationPeriods: 2,
      threshold: 300,
      comparisonOperator:
        cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
    });
  }
}
