/**
 * Order Saga — Compensation: Release inventory reservation.
 */
import { emitEvent } from './shared.js';

export const handler = async (event: any) => {
  const orderId = event.orderId;
  const items = event.items;

  await emitEvent('InventoryReleaseRequested', { orderId, items });

  return event;
};
