/**
 * Thin starter lambda: pulls SQS messages and starts the Inventory Workflow
 * Step Functions execution. Preserves SQS buffering and DLQ semantics.
 */
import { SFNClient, StartExecutionCommand } from '@aws-sdk/client-sfn';
import type { SQSHandler } from 'aws-lambda';

const sfn = new SFNClient({});
const STATE_MACHINE_ARN = process.env.STATE_MACHINE_ARN!;

export const handler: SQSHandler = async (event) => {
  for (const record of event.Records) {
    const envelope = JSON.parse(record.body);

    // Pass the full EventBridge envelope so the state machine can
    // branch on detail-type (OrderCreated vs InventoryReceived etc.)
    await sfn.send(
      new StartExecutionCommand({
        stateMachineArn: STATE_MACHINE_ARN,
        name: `inv-${Date.now()}-${record.messageId}`,
        input: JSON.stringify(envelope),
      }),
    );
  }
};
