import { randomUUID } from 'crypto';
import {
  DynamoDBClient,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import { checkIdempotency } from '../shared/idempotency.js';
import { emitEvent } from '../shared/event-emitter.js';
import { logger } from '../shared/logger.js';
import { EventType, OrderStatus } from '../shared/types.js';
import type { Channel } from '../shared/types.js';

const ddb = new DynamoDBClient({});

export const handler = async (event: any) => {
  try {
    const deviceId = event.deviceId;
    const orderId = randomUUID();
    const channel: Channel = 'pos';
    const createdAt = new Date().toISOString();

    logger.info('POS intake received', { orderId, deviceId, channel });

    // Idempotency check
    const isNew = await checkIdempotency(orderId);
    if (!isNew) {
      logger.warn('Duplicate POS order detected', { orderId, deviceId });
      return { statusCode: 200 };
    }

    // Write PENDING order to DynamoDB
    const tableName = process.env.ORDERS_TABLE;
    if (!tableName) {
      throw new Error('ORDERS_TABLE environment variable is not set');
    }

    await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: {
          pk: { S: `ORDER#${orderId}` },
          sk: { S: `ORDER#${orderId}` },
          orderId: { S: orderId },
          channel: { S: channel },
          status: { S: OrderStatus.PENDING },
          items: { S: JSON.stringify(event.items ?? []) },
          customerId: { S: event.customerId ?? '' },
          createdAt: { S: createdAt },
          deviceId: { S: deviceId ?? '' },
        },
      }),
    );

    logger.info('POS order written to DynamoDB', { orderId, deviceId, status: OrderStatus.PENDING });

    // Emit OrderCreated event
    await emitEvent(EventType.OrderCreated, {
      orderId,
      channel,
      status: OrderStatus.PENDING,
      items: event.items ?? [],
      customerId: event.customerId ?? '',
      timestamp: createdAt,
      metadata: { deviceId },
    });

    logger.info('OrderCreated event emitted for POS', { orderId, deviceId });

    return { statusCode: 200 };
  } catch (error) {
    logger.error('POS intake failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    throw error;
  }
};
