import { randomUUID, createHash } from 'crypto';
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
    const body = JSON.parse(event.body ?? '{}');
    const channel: Channel = body.channel ?? 'web';
    const createdAt = new Date().toISOString();

    // Derive a stable idempotency key from client-supplied data
    const idempKey: string = body.idempotencyKey
      ?? createHash('sha256')
          .update((body.customerId ?? '') + JSON.stringify(body.items ?? []))
          .digest('hex');

    logger.info('Order intake received', { idempKey, channel });

    // Generate orderId then run idempotency check; orderId is stored as the
    // associated value so a duplicate request can return the original orderId.
    const orderId = randomUUID();
    const isNew = await checkIdempotency(idempKey, orderId);
    if (!isNew) {
      logger.warn('Duplicate order detected', { idempKey });
      return {
        statusCode: 409,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: 'Duplicate order', idempKey }),
      };
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
          items: { S: JSON.stringify(body.items ?? []) },
          customerId: { S: body.customerId ?? '' },
          createdAt: { S: createdAt },
        },
      }),
    );

    logger.info('Order written to DynamoDB', { orderId, status: OrderStatus.PENDING });

    // Emit OrderCreated event
    await emitEvent(EventType.OrderCreated, {
      orderId,
      channel,
      status: OrderStatus.PENDING,
      items: body.items ?? [],
      customerId: body.customerId ?? '',
      timestamp: createdAt,
    });

    logger.info('OrderCreated event emitted', { orderId });

    return {
      statusCode: 202,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ orderId }),
    };
  } catch (error) {
    logger.error('Order intake failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'Internal server error' }),
    };
  }
};
