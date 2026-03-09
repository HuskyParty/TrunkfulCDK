/**
 * Order Service Lambda — durable saga-based order processing.
 *
 * Uses @aws/durable-execution-sdk-js to wrap the handler with
 * `withDurableExecution`. Each saga step (validate, reserve, pay, confirm)
 * is a durable `context.step()` that is automatically checkpointed and
 * replayed on failure.
 *
 * Receives SQS messages that wrap EventBridge events.
 */
import {
  withDurableExecution,
  type DurableExecutionHandler,
} from '@aws/durable-execution-sdk-js';
import {
  DynamoDBClient,
  UpdateItemCommand,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb';
import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';
import { checkCircuit, recordSuccess, recordFailure } from './circuit-breaker.js';
import { processPayment } from './payment-client.js';

// ---------------------------------------------------------------------------
// Clients & config
// ---------------------------------------------------------------------------

const ddb = new DynamoDBClient({});
const eb = new EventBridgeClient({});
const TABLE = process.env.ORDERS_TABLE!;
const BUS_NAME = process.env.EVENT_BUS_NAME ?? 'default';

const log = {
  info: (msg: string, extra?: Record<string, unknown>) =>
    console.log(JSON.stringify({ level: 'INFO', message: msg, ...extra })),
  error: (msg: string, extra?: Record<string, unknown>) =>
    console.error(JSON.stringify({ level: 'ERROR', message: msg, ...extra })),
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface OrderItem {
  sku: string;
  quantity: number;
}

interface Order {
  orderId: string;
  customerId: string;
  items: OrderItem[];
  totalAmount: number;
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function updateOrderStatus(
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

async function emitEvent(
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

async function releaseInventoryReservation(order: Order): Promise<void> {
  log.info('Releasing inventory reservation', { orderId: order.orderId });
  await emitEvent('InventoryReleaseRequested', {
    orderId: order.orderId,
    items: order.items,
  });
}

// ---------------------------------------------------------------------------
// Durable handler — saga steps via context.step()
// ---------------------------------------------------------------------------

const durableHandler: DurableExecutionHandler = async (event, context) => {
  // The event is the SQS record body (EventBridge envelope)
  const envelope = typeof event === 'string' ? JSON.parse(event) : event;
  const order: Order = envelope.detail ?? envelope;

  log.info('Processing order (durable)', { orderId: order.orderId });

  try {
    // Step 1 — Validate order
    await context.step('validate-order', async () => {
      if (!order.orderId) throw new Error('Missing orderId');
      if (!order.items || order.items.length === 0) throw new Error('Order has no items');
      if (order.totalAmount <= 0) throw new Error('Invalid totalAmount');
      await updateOrderStatus(order.orderId, 'VALIDATING');
      await emitEvent('OrderValidated', { orderId: order.orderId });
      log.info('Order validated', { orderId: order.orderId });
      return { validated: true };
    });

    // Step 2 — Reserve inventory
    await context.step('reserve-inventory', async () => {
      await updateOrderStatus(order.orderId, 'RESERVED');
      await emitEvent('OrderReserved', { orderId: order.orderId, items: order.items });
      log.info('Inventory reserved', { orderId: order.orderId });
      return { reserved: true };
    });

    // Step 3 — Process payment (with circuit breaker)
    const paymentResult = await context.step('process-payment', async () => {
      const circuitState = await checkCircuit('payment');
      if (circuitState === 'OPEN') {
        throw new Error('Payment circuit breaker is OPEN — refusing call');
      }

      try {
        const result = await processPayment(order.orderId, order.totalAmount);
        if (!result.success) {
          throw new Error('Payment declined');
        }
        await recordSuccess('payment');
        return result;
      } catch (err) {
        await recordFailure('payment');
        throw err;
      }
    });

    // Step 4 — Confirm order
    await context.step('confirm-order', async () => {
      await updateOrderStatus(order.orderId, 'CONFIRMED', {
        transactionId: { S: paymentResult.transactionId },
      });
      await emitEvent('OrderConfirmed', {
        orderId: order.orderId,
        customerId: order.customerId,
        totalAmount: order.totalAmount,
        transactionId: paymentResult.transactionId,
      });
      log.info('Order confirmed', { orderId: order.orderId });
      return { confirmed: true };
    });

    return { orderId: order.orderId, status: 'CONFIRMED' };
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    log.error('Order processing failed — compensating', {
      orderId: order.orderId,
      error: errorMessage,
    });

    // Compensate: release inventory reservation
    try {
      await releaseInventoryReservation(order);
    } catch (compErr) {
      log.error('Compensation (inventory release) also failed', {
        orderId: order.orderId,
        error: compErr instanceof Error ? compErr.message : String(compErr),
      });
    }

    await updateOrderStatus(order.orderId, 'FAILED');
    await emitEvent('OrderFailed', {
      orderId: order.orderId,
      reason: errorMessage,
    });

    return { orderId: order.orderId, status: 'FAILED', error: errorMessage };
  }
};

// Wrap with durable execution for automatic checkpointing & replay
export const handler = withDurableExecution(durableHandler);
