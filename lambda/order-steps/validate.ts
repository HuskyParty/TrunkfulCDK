/**
 * Order Saga — Step 1: Validate order fields and mark VALIDATING.
 */
import { updateOrderStatus, emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const order = event;

  if (!order.orderId) throw new Error('Missing orderId');
  if (!order.items || order.items.length === 0) throw new Error('Order has no items');
  if (order.totalAmount <= 0) throw new Error('Invalid totalAmount');

  await updateOrderStatus(order.orderId, 'VALIDATING');
  await emitEvent('OrderValidated', { orderId: order.orderId });

  return order;
};
