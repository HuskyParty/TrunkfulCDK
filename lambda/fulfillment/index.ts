/**
 * Fulfillment Service Lambda — stub handler.
 *
 * Receives SQS events wrapping EventBridge order events and logs that
 * fulfillment has been initiated. In production this would integrate with
 * a warehouse management / shipping system.
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

      logger.info('Fulfillment event received', {
        detailType,
        orderId: detail?.orderId,
      });

      // Stub: in production, create shipment / pick-list here
      logger.info('Fulfillment initiated', {
        orderId: detail?.orderId,
        items: detail?.items,
      });
    } catch (err) {
      logger.error('Fulfillment processing failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      throw err;
    }
  }
};
