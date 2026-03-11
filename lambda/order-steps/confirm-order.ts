/**
 * Order Saga — Step 4: Mark order CONFIRMED and emit OrderConfirmed.
 */
import { updateOrderStatus, emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const order = event;

  await updateOrderStatus(order.orderId, 'CONFIRMED', {
    transactionId: { S: order.transactionId },
  });

  await emitEvent('OrderConfirmed', {
    orderId: order.orderId,
    customerId: order.customerId,
    totalAmount: order.totalAmount,
    transactionId: order.transactionId,
  });

  return { orderId: order.orderId, status: 'CONFIRMED' };
};
