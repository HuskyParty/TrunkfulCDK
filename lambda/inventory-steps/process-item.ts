/**
 * Inventory step lambda — processes a single inventory adjustment.
 *
 * Used by both the Map state (OrderCreated — per-item reservation) and
 * the direct invocation path (generic InventoryReceived / ReturnInitiated).
 *
 * For order reservations, includes an idempotency check so that Map retries
 * do not double-adjust inventory.
 *
 * Input shape:
 *   { sku, quantity, warehouseId?, orderId?, isReservation? }
 */
import {
  DynamoDBClient,
  UpdateItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';
import {
  EventBridgeClient,
  PutEventsCommand,
} from '@aws-sdk/client-eventbridge';

const ddb = new DynamoDBClient({});
const eb = new EventBridgeClient({});
const TABLE = process.env.INVENTORY_TABLE!;
const BUS_NAME = process.env.EVENT_BUS_NAME!;
const IDEMPOTENCY_TABLE = process.env.IDEMPOTENCY_TABLE;
const LOW_STOCK_THRESHOLD = 10;

export const handler = async (event: any) => {
  const sku: string = event.sku;
  const warehouseId: string = event.warehouseId ?? 'DEFAULT';
  const rawQuantity: number = event.quantity;
  const orderId: string | undefined = event.orderId;
  const isReservation: boolean = event.isReservation === true;

  // For reservations (OrderCreated), negate the quantity
  const adjustBy = isReservation ? -Math.abs(rawQuantity) : rawQuantity;

  // Idempotency check for order reservations to prevent double-adjustment on retry
  if (orderId && IDEMPOTENCY_TABLE) {
    const idempotencyKey = `INV#${orderId}#${sku}#${warehouseId}`;
    try {
      await ddb.send(
        new PutItemCommand({
          TableName: IDEMPOTENCY_TABLE,
          Item: {
            pk: { S: idempotencyKey },
            ttl: { N: String(Math.floor(Date.now() / 1000) + 86400) },
          },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      );
    } catch (err: any) {
      if (err.name === 'ConditionalCheckFailedException') {
        return { sku, warehouseId, skipped: true };
      }
      throw err;
    }
  }

  // Atomic inventory adjustment
  const result = await ddb.send(
    new UpdateItemCommand({
      TableName: TABLE,
      Key: {
        pk: { S: `SKU#${sku}` },
        sk: { S: `WAREHOUSE#${warehouseId}` },
      },
      UpdateExpression:
        'ADD quantity :qty SET updatedAt = :now, sku = :sku, warehouseId = :wh',
      ExpressionAttributeValues: {
        ':qty': { N: String(adjustBy) },
        ':now': { S: new Date().toISOString() },
        ':sku': { S: sku },
        ':wh': { S: warehouseId },
      },
      ReturnValues: 'ALL_NEW',
    }),
  );

  const newQuantity = Number(result.Attributes?.quantity?.N ?? '0');

  // Emit InventoryAdjusted
  const reason = orderId ? `OrderCreated:${orderId}` : undefined;
  await eb.send(
    new PutEventsCommand({
      Entries: [
        {
          Source: 'trunkful.orders',
          DetailType: 'InventoryAdjusted',
          Detail: JSON.stringify({
            sku,
            warehouseId,
            adjustedBy: adjustBy,
            newQuantity,
            ...(reason && { reason }),
          }),
          EventBusName: BUS_NAME,
        },
      ],
    }),
  );

  // Emit InventoryLow if below threshold
  if (newQuantity < LOW_STOCK_THRESHOLD) {
    await eb.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: 'trunkful.orders',
            DetailType: 'InventoryLow',
            Detail: JSON.stringify({
              sku,
              warehouseId,
              currentQuantity: newQuantity,
              threshold: LOW_STOCK_THRESHOLD,
            }),
            EventBusName: BUS_NAME,
          },
        ],
      }),
    );
  }

  return { sku, warehouseId, newQuantity, lowStock: newQuantity < LOW_STOCK_THRESHOLD };
};
