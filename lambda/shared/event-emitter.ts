import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';

const client = new EventBridgeClient({});

export async function emitEvent(
  detailType: string,
  detail: Record<string, unknown>,
): Promise<void> {
  const eventBusName = process.env.EVENT_BUS_NAME;
  if (!eventBusName) {
    throw new Error('EVENT_BUS_NAME environment variable is not set');
  }

  const result = await client.send(
    new PutEventsCommand({
      Entries: [
        {
          EventBusName: eventBusName,
          Source: 'trunkful.orders',
          DetailType: detailType,
          Detail: JSON.stringify(detail),
        },
      ],
    }),
  );

  if (result.FailedEntryCount && result.FailedEntryCount > 0) {
    throw new Error(
      `Failed to put event: ${JSON.stringify(result.Entries?.[0]?.ErrorMessage)}`,
    );
  }
}
