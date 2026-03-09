import { randomUUID } from 'crypto';
import {
  DynamoDBClient,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import {
  S3Client,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import { checkIdempotency } from '../shared/idempotency.js';
import { emitEvent } from '../shared/event-emitter.js';
import { logger } from '../shared/logger.js';
import { EventType, OrderStatus } from '../shared/types.js';
import type { Channel } from '../shared/types.js';

const ddb = new DynamoDBClient({});
const s3 = new S3Client({});

export const handler = async (event: any) => {
  const channel: Channel = 'warehouse';
  let processedCount = 0;

  for (const record of event.Records ?? []) {
    const bucket = record.s3.bucket.name;
    const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));

    logger.info('Processing S3 object', { bucket, key });

    try {
      // Read file from S3
      const response = await s3.send(
        new GetObjectCommand({ Bucket: bucket, Key: key }),
      );
      const bodyString = await response.Body?.transformToString();
      if (!bodyString) {
        logger.warn('Empty S3 object, skipping', { bucket, key });
        continue;
      }

      const orders: any[] = JSON.parse(bodyString);

      const tableName = process.env.ORDERS_TABLE;
      if (!tableName) {
        throw new Error('ORDERS_TABLE environment variable is not set');
      }

      for (const order of orders) {
        const orderId = randomUUID();
        const createdAt = new Date().toISOString();

        // Idempotency check
        const isNew = await checkIdempotency(orderId);
        if (!isNew) {
          logger.warn('Duplicate warehouse order detected', { orderId });
          continue;
        }

        // Write PENDING order to DynamoDB
        await ddb.send(
          new PutItemCommand({
            TableName: tableName,
            Item: {
              pk: { S: `ORDER#${orderId}` },
              sk: { S: `ORDER#${orderId}` },
              orderId: { S: orderId },
              channel: { S: channel },
              status: { S: OrderStatus.PENDING },
              items: { S: JSON.stringify(order.items ?? []) },
              customerId: { S: order.customerId ?? '' },
              createdAt: { S: createdAt },
            },
          }),
        );

        // Emit OrderCreated event
        await emitEvent(EventType.OrderCreated, {
          orderId,
          channel,
          status: OrderStatus.PENDING,
          items: order.items ?? [],
          customerId: order.customerId ?? '',
          timestamp: createdAt,
          metadata: { sourceBucket: bucket, sourceKey: key },
        });

        processedCount++;
      }
    } catch (error) {
      logger.error('Failed to process S3 object', {
        bucket,
        key,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  logger.info('Admin ingest complete', { processedCount });

  return { statusCode: 200, processedCount };
};
