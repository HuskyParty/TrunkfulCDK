/**
 * Payment provider stub.
 *
 * In production this would call an external payment gateway (Stripe, Adyen, etc.).
 * For now it always returns success with a generated transaction ID.
 */

const logger = {
  info: (msg: string, extra?: Record<string, unknown>) =>
    console.log(JSON.stringify({ level: 'INFO', message: msg, ...extra })),
};

export async function processPayment(
  orderId: string,
  amount: number,
): Promise<{ success: boolean; transactionId: string }> {
  logger.info('Processing payment', { orderId, amount });

  // Simulate a short processing delay
  await new Promise((resolve) => setTimeout(resolve, 50));

  const transactionId = `txn_${Date.now()}_${orderId}`;
  logger.info('Payment processed successfully', { orderId, transactionId });

  return { success: true, transactionId };
}
