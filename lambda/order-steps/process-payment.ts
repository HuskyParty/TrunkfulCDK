/**
 * Order Saga — Step 3: Process payment with circuit breaker protection.
 */
import { checkCircuit, recordSuccess, recordFailure } from '../order-service/circuit-breaker.js';
import { processPayment } from '../order-service/payment-client.js';

export const handler = async (event: any) => {
  const order = event;

  const circuitState = await checkCircuit('payment');
  if (circuitState === 'OPEN') {
    throw new Error('Payment circuit breaker is OPEN — refusing call');
  }

  try {
    const result = await processPayment(order.orderId, order.totalAmount, order.orderId);
    if (!result.success) {
      throw new Error('Payment declined');
    }
    await recordSuccess('payment');
    return { ...order, transactionId: result.transactionId };
  } catch (err) {
    await recordFailure('payment');
    throw err;
  }
};
