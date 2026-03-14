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
    // Validate webhook secret
    const webhookSecret = process.env.WEBHOOK_SECRET;
    const headerSecret =
      event.headers?.['X-Webhook-Secret'] ??
      event.headers?.['x-webhook-secret'];

    if (!webhookSecret || headerSecret !== webhookSecret) {
      logger.warn('Unauthorized webhook request', {
        hasSecret: !!headerSecret,
      });
      return {
        statusCode: 401,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: 'Unauthorized' }),
      };
    }

    const body = JSON.parse(event.body ?? '{}');
    const orderId = randomUUID();
    const channel: Channel = 'supplier';
    const createdAt = new Date().toISOString();

    logger.info('Webhook intake received', { orderId, channel });

    // Idempotency check
    const isNew = await checkIdempotency(orderId, orderId);
    if (!isNew) {
      logger.warn('Duplicate webhook order detected', { orderId });
      return {
        statusCode: 409,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: 'Duplicate order', orderId }),
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

    logger.info('Webhook order written to DynamoDB', { orderId, status: OrderStatus.PENDING });

    // Emit OrderCreated event
    await emitEvent(EventType.OrderCreated, {
      orderId,
      channel,
      status: OrderStatus.PENDING,
      items: body.items ?? [],
      customerId: body.customerId ?? '',
      timestamp: createdAt,
    });

    logger.info('OrderCreated event emitted for webhook', { orderId });

    return {
      statusCode: 202,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ orderId }),
    };
  } catch (error) {
    logger.error('Webhook intake failed', {
      error: error instanceof Error ? error.message : String(error),
    });
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message: 'Internal server error' }),
    };
  }
};
