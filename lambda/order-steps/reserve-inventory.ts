/**
 * Order Saga — Step 2: Conditionally decrement inventory in DynamoDB and
 * mark the order as RESERVED.
 *
 * Uses a condition expression (`stock >= :quantity`) so the update is rejected
 * when there is insufficient stock, causing the saga to roll back.
 */
import {
  DynamoDBClient,
  UpdateItemCommand,
  ConditionalCheckFailedException,
} from '@aws-sdk/client-dynamodb';
import { updateOrderStatus, emitEvent } from './shared.js';

const ddb = new DynamoDBClient({});
const INVENTORY_TABLE = process.env.INVENTORY_TABLE!;

export const handler = async (event: any) => {
  const order = event;

  // Decrement stock for every line item using a conditional update.
  for (const item of order.items as { sku: string; quantity: number }[]) {
    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: INVENTORY_TABLE,
          Key: {
            pk: { S: `SKU#${item.sku}` },
            sk: { S: `SKU#${item.sku}` },
          },
          UpdateExpression: 'SET stock = stock - :quantity',
          ConditionExpression: 'stock >= :quantity',
          ExpressionAttributeValues: {
            ':quantity': { N: String(item.quantity) },
          },
        }),
      );
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) {
        throw new Error(
          `Insufficient stock for SKU ${item.sku} (requested ${item.quantity})`,
        );
      }
      throw err;
    }
  }

  await updateOrderStatus(order.orderId, 'RESERVED');
  await emitEvent('OrderReserved', {
    orderId: order.orderId,
    items: order.items,
  });

  return order;
};
