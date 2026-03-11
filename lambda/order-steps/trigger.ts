/**
 * Thin starter lambda: pulls SQS messages and starts the Order Saga
 * Step Functions execution. Preserves SQS buffering and DLQ semantics.
 */
import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';
import type { SQSHandler } from 'aws-lambda';

const sfn = new SFNClient({});
const STATE_MACHINE_ARN = process.env.STATE_MACHINE_ARN!;

export const handler: SQSHandler = async (event) => {
  for (const record of event.Records) {
    const envelope = JSON.parse(record.body);
    const order = envelope.detail ?? envelope;

    await sfn.send(
      new StartExecutionCommand({
        stateMachineArn: STATE_MACHINE_ARN,
        name: `order-${order.orderId}-${Date.now()}`,
        input: JSON.stringify(order),
      }),
    );
  }
};
