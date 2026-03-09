/**
 * Billing Service Lambda — stub handler.
 *
 * Receives SQS events wrapping EventBridge order events and logs that
 * billing has been processed. In production this would integrate with
 * an invoicing / accounting system.
 */
import type { SQSEvent } from 'aws-lambda';

const logger = {
  info: (msg: string, extra?: Record<string, unknown>) =>
    console.log(JSON.stringify({ level: 'INFO', message: msg, ...extra })),
  error: (msg: string, extra?: Record<string, unknown>) =>
    console.error(JSON.stringify({ level: 'ERROR', message: msg, ...extra })),
};

export const handler = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    try {
      const envelope = JSON.parse(record.body);
      const detail = envelope.detail;
      const detailType: string =
        envelope['detail-type'] ?? envelope.detailType ?? 'Unknown';

      logger.info('Billing event received', {
        detailType,
        orderId: detail?.orderId,
        customerId: detail?.customerId,
        totalAmount: detail?.totalAmount,
      });

      // Stub: in production, create invoice / charge record here
      logger.info('Billing processed', { orderId: detail?.orderId });
    } catch (err) {
      logger.error('Billing processing failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  }
};
