import { Construct } from 'constructs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import { Duration } from 'aws-cdk-lib';

interface QueuesProps {
  stageName: string;
}

export class QueuesConstruct extends Construct {
  public readonly orderQueue: sqs.Queue;
  public readonly inventoryQueue: sqs.Queue;
  public readonly billingQueue: sqs.Queue;
  public readonly fulfillmentQueue: sqs.Queue;
  public readonly notificationQueue: sqs.Queue;

  constructor(scope: Construct, id: string, props: QueuesProps) {
    super(scope, id);

    const prefix = props.stageName;

    // Helper to create a main queue with its corresponding DLQ
    const createQueueWithDlq = (
      idPrefix: string,
      queueName: string,
      dlqName: string,
    ): sqs.Queue => {
      const dlq = new sqs.Queue(this, `${idPrefix}Dlq`, {
        queueName: `${prefix}-${dlqName}`,
        retentionPeriod: Duration.days(14),
      });

      return new sqs.Queue(this, `${idPrefix}Queue`, {
        queueName: `${prefix}-${queueName}`,
        visibilityTimeout: Duration.seconds(60),
        retentionPeriod: Duration.days(14),
        deadLetterQueue: {
          queue: dlq,
          maxReceiveCount: 3,
        },
      });
    };

    this.orderQueue = createQueueWithDlq('Order', 'order-queue', 'order-dlq');
    this.inventoryQueue = createQueueWithDlq('Inventory', 'inventory-queue', 'inventory-dlq');
    this.billingQueue = createQueueWithDlq('Billing', 'billing-queue', 'billing-dlq');
    this.fulfillmentQueue = createQueueWithDlq('Fulfillment', 'fulfillment-queue', 'fulfillment-dlq');
    this.notificationQueue = createQueueWithDlq('Notification', 'notification-queue', 'notification-dlq');
  }
}
