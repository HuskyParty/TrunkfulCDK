/**
 * Inventory Service Lambda — durable inventory adjustment processing.
 *
 * Uses @aws/durable-execution-sdk-js to wrap the handler with
 * `withDurableExecution`. Each inventory operation is a durable
 * `context.step()` that is automatically checkpointed.
 *
 * Handles two flows:
 *   1. OrderCreated events — reserves stock by decrementing for each item
 *   2. Generic inventory adjustments (InventoryAdjusted, InventoryReceived, etc.)
 *
 * Emits InventoryAdjusted after every update and InventoryLow when quantity
 * drops below the configured threshold.
 */
import {
  withDurableExecution,
  type DurableExecutionHandler,
} from '@aws/durable-execution-sdk-js';
import {
  DynamoDBClient,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb';
import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';

// ---------------------------------------------------------------------------
// Clients & config
// ---------------------------------------------------------------------------

const ddb = new DynamoDBClient({});
const eb = new EventBridgeClient({});
const TABLE = process.env.INVENTORY_TABLE!;
const BUS_NAME = process.env.EVENT_BUS_NAME ?? 'default';
const LOW_STOCK_THRESHOLD = 10;

const log = {
  info: (msg: string, extra?: Record<string, unknown>) =>
    console.log(JSON.stringify({ level: 'INFO', message: msg, ...extra })),
  error: (msg: string, extra?: Record<string, unknown>) =>
    console.error(JSON.stringify({ level: 'ERROR', message: msg, ...extra })),
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface InventoryAdjustment {
  sku: string;
  warehouseId: string;
  quantity: number;
}

interface OrderItem {
  sku: string;
  quantity: number;
  warehouseId?: string;
}

interface OrderDetail {
  orderId: string;
  items: OrderItem[];
  [key: string]: unknown;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function adjustInventory(adj: InventoryAdjustment): Promise<number> {
  const result = await ddb.send(
    new UpdateItemCommand({
      TableName: TABLE,
      Key: {
        pk: { S: `SKU#${adj.sku}` },
        sk: { S: `WAREHOUSE#${adj.warehouseId}` },
      },
      UpdateExpression:
        'ADD quantity :qty SET updatedAt = :now, sku = :sku, warehouseId = :wh',
      ExpressionAttributeValues: {
        ':qty': { N: String(adj.quantity) },
        ':now': { S: new Date().toISOString() },
        ':sku': { S: adj.sku },
        ':wh': { S: adj.warehouseId },
      },
      ReturnValues: 'ALL_NEW',
    }),
  );

  return Number(result.Attributes?.quantity?.N ?? '0');
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

// ---------------------------------------------------------------------------
// Durable handler
// ---------------------------------------------------------------------------

const durableHandler: DurableExecutionHandler = async (event, context) => {
  const envelope = typeof event === 'string' ? JSON.parse(event) : event;
  const detailType: string = envelope['detail-type'] ?? envelope.detailType ?? '';
  const detail = envelope.detail ?? envelope;

  try {
    if (detailType === 'OrderCreated') {
      const order: OrderDetail = detail;
      log.info('Reserving stock for order (durable)', { orderId: order.orderId });

      for (let i = 0; i < order.items.length; i++) {
        const item = order.items[i];
        const warehouseId = item.warehouseId ?? 'DEFAULT';

        // Each item reservation is a durable step
        const newQty = await context.step(
          `reserve-${item.sku}-${warehouseId}`,
          async () => {
            return adjustInventory({
              sku: item.sku,
              warehouseId,
              quantity: -item.quantity,
            });
          },
        );

        log.info('Stock reserved', {
          sku: item.sku,
          warehouseId,
          reserved: item.quantity,
          remaining: newQty,
        });

        // Emit adjustment event as a durable step
        await context.step(
          `emit-adjusted-${item.sku}-${warehouseId}`,
          async () => {
            await emitEvent('InventoryAdjusted', {
              sku: item.sku,
              warehouseId,
              adjustedBy: -item.quantity,
              newQuantity: newQty,
              reason: `OrderCreated:${order.orderId}`,
            });
          },
        );

        if (newQty < LOW_STOCK_THRESHOLD) {
          await context.step(
            `emit-low-${item.sku}-${warehouseId}`,
            async () => {
              log.info('Low stock detected', { sku: item.sku, quantity: newQty });
              await emitEvent('InventoryLow', {
                sku: item.sku,
                warehouseId,
                currentQuantity: newQty,
                threshold: LOW_STOCK_THRESHOLD,
              });
            },
          );
        }
      }

      return { orderId: order.orderId, status: 'reserved' };
    } else {
      // Generic inventory adjustment
      const adj: InventoryAdjustment = detail;
      log.info('Processing inventory adjustment (durable)', {
        sku: adj.sku,
        warehouseId: adj.warehouseId,
        quantity: adj.quantity,
      });

      const newQty = await context.step('adjust-inventory', async () => {
        return adjustInventory(adj);
      });

      await context.step('emit-adjusted', async () => {
        await emitEvent('InventoryAdjusted', {
          sku: adj.sku,
          warehouseId: adj.warehouseId,
          adjustedBy: adj.quantity,
          newQuantity: newQty,
        });
      });

      log.info('Inventory adjusted', { sku: adj.sku, newQuantity: newQty });

      if (newQty < LOW_STOCK_THRESHOLD) {
        await context.step('emit-low-stock', async () => {
          log.info('Low stock detected', { sku: adj.sku, quantity: newQty });
          await emitEvent('InventoryLow', {
            sku: adj.sku,
            warehouseId: adj.warehouseId,
            currentQuantity: newQty,
            threshold: LOW_STOCK_THRESHOLD,
          });
        });
      }

      return { sku: adj.sku, newQuantity: newQty };
    }
  } catch (err) {
    log.error('Inventory processing failed', {
      error: err instanceof Error ? err.message : String(err),
      detailType,
    });
    throw err; // Let SQS retry / DLQ handle it
  }
};

// Wrap with durable execution for automatic checkpointing & replay
export const handler = withDurableExecution(durableHandler);
