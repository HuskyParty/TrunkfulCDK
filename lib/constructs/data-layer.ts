import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import { RemovalPolicy } from 'aws-cdk-lib';

interface DataLayerProps {
  stageName: string;
  removalPolicy: RemovalPolicy;
}

export class DataLayerConstruct extends Construct {
  public readonly ordersTable: dynamodb.Table;
  public readonly inventoryTable: dynamodb.Table;
  public readonly idempotencyTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: DataLayerProps) {
    super(scope, id);

    // Orders table with single-table design keys
    this.ordersTable = new dynamodb.Table(this, 'OrdersTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: props.removalPolicy,
    });

    // GSI1: look up orders by orderId sorted by createdAt
    this.ordersTable.addGlobalSecondaryIndex({
      indexName: 'GSI1',
      partitionKey: { name: 'orderId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING },
    });

    // Inventory table
    this.inventoryTable = new dynamodb.Table(this, 'InventoryTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      removalPolicy: props.removalPolicy,
    });

    // Idempotency table with TTL for automatic expiry of processed event records
    this.idempotencyTable = new dynamodb.Table(this, 'IdempotencyTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      removalPolicy: props.removalPolicy,
    });
  }
}
