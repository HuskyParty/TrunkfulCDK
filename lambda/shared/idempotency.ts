import {
  DynamoDBClient,
  PutItemCommand,
  ConditionalCheckFailedException,
} from '@aws-sdk/client-dynamodb';

const client = new DynamoDBClient({});

export async function checkIdempotency(idempotencyKey: string): Promise<boolean> {
  const tableName = process.env.IDEMPOTENCY_TABLE;
  if (!tableName) {
    throw new Error('IDEMPOTENCY_TABLE environment variable is not set');
  }

  const ttl = Math.floor(Date.now() / 1000) + 24 * 60 * 60;

  try {
    await client.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          pk: { S: idempotencyKey },
          ttl: { N: String(ttl) },
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
