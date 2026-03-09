/**
 * Notification Service Lambda — routes events to the appropriate
 * notification channel (email, ops alert, etc.).
 *
 * Uses KMS to decrypt PII fields when present, then dispatches based on
 * the event's detail-type.
 */
import type { SQSEvent } from 'aws-lambda';
import { KMSClient, DecryptCommand } from '@aws-sdk/client-kms';

// ---------------------------------------------------------------------------
// Clients & config
// ---------------------------------------------------------------------------

const kms = new KMSClient({});
const KMS_KEY_ID = process.env.KMS_KEY_ID;

const logger = {
  info: (msg: string, extra?: Record<string, unknown>) =>
    console.log(JSON.stringify({ level: 'INFO', message: msg, ...extra })),
  warn: (msg: string, extra?: Record<string, unknown>) =>
    console.warn(JSON.stringify({ level: 'WARN', message: msg, ...extra })),
  error: (msg: string, extra?: Record<string, unknown>) =>
    console.error(JSON.stringify({ level: 'ERROR', message: msg, ...extra })),
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * Attempt to decrypt a base-64-encoded ciphertext blob using KMS.
 * Returns the plaintext string, or the original value if decryption is
 * not configured or fails.
 */
async function decryptIfEncrypted(value: string): Promise<string> {
  if (!KMS_KEY_ID) return value;

  try {
    const cipherBlob = Buffer.from(value, 'base64');
    const result = await kms.send(
      new DecryptCommand({
        CiphertextBlob: cipherBlob,
        KeyId: KMS_KEY_ID,
      }),
    );
    if (result.Plaintext) {
      return Buffer.from(result.Plaintext).toString('utf-8');
    }
  } catch {
    // Value was likely not encrypted — return as-is
    logger.warn('KMS decryption skipped or failed; using raw value');
  }

  return value;
}

// ---------------------------------------------------------------------------
// Main handler
// ---------------------------------------------------------------------------

export const handler = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    try {
      const envelope = JSON.parse(record.body);
      const detailType: string =
        envelope['detail-type'] ?? envelope.detailType ?? 'Unknown';
      const detail = envelope.detail ?? {};

      logger.info('Notification event received', { detailType });

      // Decrypt PII fields if present
      const email: string | undefined = detail.email
        ? await decryptIfEncrypted(detail.email)
        : undefined;
      const customerName: string | undefined = detail.customerName
        ? await decryptIfEncrypted(detail.customerName)
        : undefined;

      switch (detailType) {
        case 'OrderConfirmed':
          logger.info('Sending confirmation email', {
            orderId: detail.orderId,
            customerId: detail.customerId,
            email,
            customerName,
          });
          // Stub: SES SendEmail would go here
          break;

        case 'OrderFailed':
          logger.info('Sending failure notification', {
            orderId: detail.orderId,
            reason: detail.reason,
            email,
          });
          // Stub: SES SendEmail with failure template
          break;

        case 'InventoryLow':
          logger.info('Sending inventory alert to ops team', {
            sku: detail.sku,
            warehouseId: detail.warehouseId,
            currentQuantity: detail.currentQuantity,
            threshold: detail.threshold,
          });
          // Stub: SNS publish to ops topic or SES to ops team
          break;

        default:
          logger.warn('Unhandled notification event type', { detailType });
      }
    } catch (err) {
      logger.error('Notification processing failed', {
        error: err instanceof Error ? err.message : String(err),
      });
      // Swallow error so the message is removed from the queue.
      // In production, consider DLQ for retry.
    }
  }
};
