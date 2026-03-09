export enum OrderStatus {
  PENDING = 'PENDING',
  VALIDATING = 'VALIDATING',
  RESERVED = 'RESERVED',
  PROCESSING = 'PROCESSING',
  CONFIRMED = 'CONFIRMED',
  FAILED = 'FAILED',
}

export enum EventType {
  OrderCreated = 'OrderCreated',
  OrderValidated = 'OrderValidated',
  OrderReserved = 'OrderReserved',
  OrderConfirmed = 'OrderConfirmed',
  OrderFailed = 'OrderFailed',
  InventoryReceived = 'InventoryReceived',
  InventoryAdjusted = 'InventoryAdjusted',
  InventoryLow = 'InventoryLow',
  ReturnInitiated = 'ReturnInitiated',
}

export type Channel = 'web' | 'mobile' | 'pos' | 'warehouse' | 'supplier';

export interface OrderItem {
  sku: string;
  quantity: number;
  price: number;
}

export interface OrderEvent {
  orderId: string;
  channel: Channel;
  status: OrderStatus;
  items: OrderItem[];
  customerId: string;
  timestamp: string;
  metadata?: Record<string, unknown>;
}

export interface InventoryEvent {
  sku: string;
  warehouseId: string;
  quantity: number;
  adjustmentType: 'receive' | 'adjust' | 'reserve' | 'release';
  timestamp: string;
}
