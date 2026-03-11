/**
 * Order Saga — Terminal failure: Mark order FAILED and emit OrderFailed.
 */
import { updateOrderStatus, emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const orderId = event.orderId;
  const errorInfo = event.error;
  const reason = errorInfo?.Cause || errorInfo?.Error || 'Unknown error';

  await updateOrderStatus(orderId, 'FAILED');
  await emitEvent('OrderFailed', { orderId, reason });

  return { orderId, status: 'FAILED', reason };
};
