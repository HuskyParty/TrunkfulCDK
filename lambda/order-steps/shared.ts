/**
 * Shared helpers for order saga step lambdas.
 */
import {
  DynamoDBClient,
  UpdateItemCommand,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb';
import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';

export const ddb = new DynamoDBClient({});
export const eb = new EventBridgeClient({});
export const TABLE = process.env.ORDERS_TABLE!;
export const BUS_NAME = process.env.EVENT_BUS_NAME!;

export async function updateOrderStatus(
  orderId: string,
  status: string,
  extra?: Record<string, AttributeValue>,
): Promise<void> {
  let expression = 'SET #status = :status, updatedAt = :now';
  const names: Record<string, string> = { '#status': 'status' };
  const values: Record<string, AttributeValue> = {
    ':status': { S: status },
    ':now': { S: new Date().toISOString() },
  };

  if (extra) {
    for (const [alias, val] of Object.entries(extra)) {
      expression += `, ${alias} = :${alias}`;
      values[`:${alias}`] = val;
    }
  }

  await ddb.send(
    new UpdateItemCommand({
      TableName: TABLE,
      Key: {
        pk: { S: `ORDER#${orderId}` },
        sk: { S: `ORDER#${orderId}` },
      },
      UpdateExpression: expression,
      ExpressionAttributeNames: names,
      ExpressionAttributeValues: values,
    }),
  );
}

export async function emitEvent(
  detailType: string,
  detail: Record<string, unknown>,
): Promise<void> {
  await eb.send(
    new PutEventsCommand({
      Entries: [
        {
          Source: 'trunkful.orders',
          DetailType: detailType,
          Detail: JSON.stringify(detail),
          EventBusName: BUS_NAME,
        },
      ],
    }),
  );
}
