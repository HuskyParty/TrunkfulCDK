import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';
import { Duration, RemovalPolicy } from 'aws-cdk-lib';

interface AnalyticsProps {
  stageName: string;
  eventBus: events.EventBus;
}

export class AnalyticsConstruct extends Construct {
  public readonly analyticsDeliveryStream: firehose.CfnDeliveryStream;

  constructor(scope: Construct, id: string, props: AnalyticsProps) {
    super(scope, id);

    // ---------------------------------------------------------------
    // 1. S3 Data Lake Bucket
    // ---------------------------------------------------------------
    const dataLakeBucket = new s3.Bucket(this, 'DataLakeBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      lifecycleRules: [
        {
          transitions: [
            {
              storageClass: s3.StorageClass.INFREQUENT_ACCESS,
              transitionAfter: Duration.days(30),
            },
            {
              storageClass: s3.StorageClass.GLACIER,
              transitionAfter: Duration.days(90),
            },
          ],
        },
      ],
      removalPolicy: RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // ---------------------------------------------------------------
    // 2. IAM Role for Firehose → S3
    // ---------------------------------------------------------------
    const firehoseRole = new iam.Role(this, 'FirehoseDeliveryRole', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    dataLakeBucket.grantReadWrite(firehoseRole);
    dataLakeBucket.grantPut(firehoseRole);

    firehoseRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          's3:AbortMultipartUpload',
          's3:GetBucketLocation',
          's3:GetObject',
          's3:ListBucket',
          's3:ListBucketMultipartUploads',
          's3:PutObject',
        ],
        resources: [
          dataLakeBucket.bucketArn,
          `${dataLakeBucket.bucketArn}/*`,
        ],
      }),
    );

    firehoseRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'glue:GetTable',
          'glue:GetTableVersion',
          'glue:GetTableVersions',
        ],
        resources: ['*'],
      }),
    );

    // ---------------------------------------------------------------
    // 3. Glue Database (moved before Firehose so the ARN is available)
    // ---------------------------------------------------------------
    const glueDatabase = new glue.CfnDatabase(this, 'AnalyticsDatabase', {
      catalogId: this.node.tryGetContext('aws:cdk:account') || '',
      databaseInput: {
        name: `${props.stageName}_trunkful_analytics`,
      },
    });

    // ---------------------------------------------------------------
    // 4. Glue Table (Parquet)
    // ---------------------------------------------------------------
    const glueTable = new glue.CfnTable(this, 'OrderEventsTable', {
      catalogId: glueDatabase.catalogId,
      databaseName: `${props.stageName}_trunkful_analytics`,
      tableInput: {
        name: 'order_events',
        tableType: 'EXTERNAL_TABLE',
        parameters: {
          classification: 'parquet',
          'parquet.compression': 'SNAPPY',
        },
        storageDescriptor: {
          location: `s3://${dataLakeBucket.bucketName}/events/`,
          inputFormat:
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
          outputFormat:
            'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
          serdeInfo: {
            serializationLibrary:
              'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
            parameters: {
              'serialization.format': '1',
            },
          },
          columns: [
            { name: 'orderId', type: 'string' },
            { name: 'channel', type: 'string' },
            { name: 'status', type: 'string' },
            { name: 'customerId', type: 'string' },
            { name: 'timestamp', type: 'string' },
            { name: 'eventType', type: 'string' },
            { name: 'amount', type: 'double' },
            { name: 'currency', type: 'string' },
            { name: 'items', type: 'string' },
          ],
        },
        partitionKeys: [
          { name: 'year', type: 'string' },
          { name: 'month', type: 'string' },
          { name: 'day', type: 'string' },
          { name: 'eventType', type: 'string' },
        ],
      },
    });
    glueTable.addDependency(glueDatabase);

    // ---------------------------------------------------------------
    // 5. Firehose Delivery Stream (L1 CfnDeliveryStream)
    // ---------------------------------------------------------------
    this.analyticsDeliveryStream = new firehose.CfnDeliveryStream(
      this,
      'AnalyticsDeliveryStream',
      {
        deliveryStreamName: `${props.stageName}-TrunkfulAnalyticsStream`,
        extendedS3DestinationConfiguration: {
          bucketArn: dataLakeBucket.bucketArn,
          roleArn: firehoseRole.roleArn,
          prefix:
            'events/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/eventType=!{partitionKeyFromQuery:eventType}/',
          errorOutputPrefix: 'errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/',
          bufferingHints: {
            intervalInSeconds: 60,
            sizeInMBs: 1,
          },
          dynamicPartitioningConfiguration: {
            enabled: true,
          },
          processingConfiguration: {
            enabled: true,
            processors: [
              {
                type: 'MetadataExtraction',
                parameters: [
                  {
                    parameterName: 'MetadataExtractionQuery',
                    parameterValue: '{eventType:.eventType}',
                  },
                  {
                    parameterName: 'JsonParsingEngine',
                    parameterValue: 'JQ-1.6',
                  },
                ],
              },
            ],
          },
          dataFormatConversionConfiguration: {
            enabled: true,
            inputFormatConfiguration: {
              deserializer: {
                openXJsonSerDe: {},
              },
            },
            outputFormatConfiguration: {
              serializer: {
                parquetSerDe: {},
              },
            },
            schemaConfiguration: {
              roleArn: firehoseRole.roleArn,
              databaseName: `${props.stageName}_trunkful_analytics`,
              tableName: 'order_events',
              region: this.node.tryGetContext('aws:cdk:region') || 'us-east-1',
              versionId: 'LATEST',
            },
          },
        },
      },
    );
    this.analyticsDeliveryStream.addDependency(glueTable);

    // ---------------------------------------------------------------
    // 6. Athena WorkGroup
    // ---------------------------------------------------------------
    new athena.CfnWorkGroup(this, 'AnalyticsWorkGroup', {
      name: `${props.stageName}-TrunkfulAnalytics`,
      workGroupConfiguration: {
        resultConfiguration: {
          outputLocation: `s3://${dataLakeBucket.bucketName}/athena-results/`,
        },
        enforceWorkGroupConfiguration: true,
        bytesScannedCutoffPerQuery: 10737418240, // 10 GB
      },
    });
  }
}
