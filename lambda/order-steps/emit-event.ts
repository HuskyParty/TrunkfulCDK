/**
 * Order Saga — Emit a named event to EventBridge.
 *
 * Expects the Step Functions input to carry an `eventType` field (e.g.
 * "OrderConfirmed" or "OrderFailed") so the same Lambda can be reused for
 * both terminal states.
 */
import { emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const { eventType, ...rest } = event;

  if (!eventType) {
    throw new Error('emit-event: missing required field "eventType"');
  }

  await emitEvent(eventType, rest);

  return event;
};
