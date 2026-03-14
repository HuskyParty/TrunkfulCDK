import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as kms from 'aws-cdk-lib/aws-kms';
import { RemovalPolicy } from 'aws-cdk-lib';

interface DataLayerProps {
  stageName: string;
  removalPolicy: RemovalPolicy;
  piiKey: kms.Key;
}

export class DataLayerConstruct extends Construct {
  public readonly ordersTable: dynamodb.Table;
  public readonly inventoryTable: dynamodb.Table;
  public readonly idempotencyTable: dynamodb.Table;
  public readonly customerTable: dynamodb.Table;

  constructor(scope: Construct, id: string, props: DataLayerProps) {
    super(scope, id);

    // Orders table with single-table design keys
    this.ordersTable = new dynamodb.Table(this, 'OrdersTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      removalPolicy: props.removalPolicy,
    });

    // GSI1: look up orders by customerId sorted by createdAt (customer order history)
    this.ordersTable.addGlobalSecondaryIndex({
      indexName: 'GSI1',
      partitionKey: { name: 'customerId', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING },
    });

    // GSI2: look up orders by status sorted by createdAt (operational queries)
    this.ordersTable.addGlobalSecondaryIndex({
      indexName: 'GSI2',
      partitionKey: { name: 'status', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'createdAt', type: dynamodb.AttributeType.STRING },
    });

    // Customer / PII Store table — encrypted with the PII CMK, PITR enabled
    this.customerTable = new dynamodb.Table(this, 'CustomerTable', {
      partitionKey: { name: 'customerId', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      encryptionKey: props.piiKey,
      removalPolicy: props.removalPolicy,
    });

    // Inventory table
    this.inventoryTable = new dynamodb.Table(this, 'InventoryTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'sk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      pointInTimeRecovery: true,
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      removalPolicy: props.removalPolicy,
    });

    // Idempotency table with TTL for automatic expiry of processed event records
    this.idempotencyTable = new dynamodb.Table(this, 'IdempotencyTable', {
      partitionKey: { name: 'pk', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: 'ttl',
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      removalPolicy: props.removalPolicy,
    });
  }
}
