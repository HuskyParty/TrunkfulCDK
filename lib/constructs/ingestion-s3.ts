import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as events from 'aws-cdk-lib/aws-events';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Duration, RemovalPolicy } from 'aws-cdk-lib';
import * as path from 'path';

interface IngestionS3Props {
  stageName: string;
  ordersTable: dynamodb.Table;
  idempotencyTable: dynamodb.Table;
  eventBus: events.EventBus;
}

export class IngestionS3Construct extends Construct {
  public readonly uploadBucket: s3.Bucket;

  constructor(scope: Construct, id: string, props: IngestionS3Props) {
    super(scope, id);

    // ---------------------------------------------------------------
    // 1. S3 Bucket for Admin Uploads
    // ---------------------------------------------------------------
    this.uploadBucket = new s3.Bucket(this, 'AdminUploadBucket', {
      bucketName: undefined, // auto-generated unique name
      encryption: s3.BucketEncryption.S3_MANAGED,
      versioned: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        {
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: Duration.days(30),
            },
          ],
          expiration: Duration.days(90),
        },
      ],
    });

    // ---------------------------------------------------------------
    // 2. Admin Ingest Lambda
    // ---------------------------------------------------------------
    const adminIngestFn = new NodejsFunction(this, 'AdminIngestFn', {
      functionName: `${props.stageName}-trunkful-admin-ingest`,
      entry: path.join(__dirname, '../../lambda/admin-ingest/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(60),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
    });

    props.ordersTable.grantReadWriteData(adminIngestFn);
    props.idempotencyTable.grantReadWriteData(adminIngestFn);
    props.eventBus.grantPutEventsTo(adminIngestFn);
    this.uploadBucket.grantRead(adminIngestFn);

    // ---------------------------------------------------------------
    // 3. S3 Event Notification → Lambda on OBJECT_CREATED
    // ---------------------------------------------------------------
    this.uploadBucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(adminIngestFn),
    );
  }
}
