/**
 * Order Saga — Step 2: Mark inventory as RESERVED and emit event.
 */
import { updateOrderStatus, emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const order = event;

  await updateOrderStatus(order.orderId, 'RESERVED');
  await emitEvent('OrderReserved', {
    orderId: order.orderId,
    items: order.items,
  });

  return order;
};
