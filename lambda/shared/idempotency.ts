import {
  DynamoDBClient,
  PutItemCommand,
  ConditionalCheckFailedException,
} from '@aws-sdk/client-dynamodb';

const client = new DynamoDBClient({});

export async function checkIdempotency(
  idempotencyKey: string,
  orderId: string,
  status: string = 'RECEIVED',
): Promise<boolean> {
  const tableName = process.env.IDEMPOTENCY_TABLE;
  if (!tableName) {
    throw new Error('IDEMPOTENCY_TABLE environment variable is not set');
  }

  const ttl = Math.floor(Date.now() / 1000) + 24 * 60 * 60;
  const createdAt = new Date().toISOString();

  try {
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          pk: { S: idempotencyKey },
          ttl: { N: String(ttl) },
          orderId: { S: orderId },
          status: { S: status },
          createdAt: { S: createdAt },
        },
        ConditionExpression: 'attribute_not_exists(pk)',
      }),
    );
    return true;
  } catch (error) {
    if (error instanceof ConditionalCheckFailedException) {
      return false;
    }
    throw error;
  }
}
