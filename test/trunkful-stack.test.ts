import * as cdk from 'aws-cdk-lib';
import { Template } from 'aws-cdk-lib/assertions';
import { TrunkfulStack } from '../lib/trunkful-stack';
import { ALPHA_CONFIG } from '../lib/stage-config';

describe('TrunkfulStack', () => {
  let template: Template;

  beforeAll(() => {
    const app = new cdk.App();
    const stack = new TrunkfulStack(app, 'TestStack', {
      stageConfig: ALPHA_CONFIG,
    });
    template = Template.fromStack(stack);
  });

  test('synthesizes without errors', () => {
    expect(template).toBeDefined();
  });

  test('creates DynamoDB tables', () => {
    template.resourceCountIs('AWS::DynamoDB::Table', 3);
  });

  test('creates Orders table with GSI', () => {
    template.hasResourceProperties('AWS::DynamoDB::Table', {
      GlobalSecondaryIndexes: [
        {
          IndexName: 'GSI1',
        },
      ],
    });
  });

  test('creates custom EventBridge bus with stage prefix', () => {
    template.hasResourceProperties('AWS::Events::EventBus', {
      Name: 'alpha-TrunkfulEventBus',
    });
  });

  test('creates SQS queues with DLQs', () => {
    // 5 main queues + 5 DLQs = 10
    template.resourceCountIs('AWS::SQS::Queue', 10);
  });

  test('creates EventBridge rules', () => {
    // 6 rules: order, inventory, billing, fulfillment, notification, firehose
    template.resourceCountIs('AWS::Events::Rule', 6);
  });

  test('creates KMS key for PII encryption', () => {
    template.hasResourceProperties('AWS::KMS::Key', {
      EnableKeyRotation: true,
    });
  });

  test('creates REST API Gateway with stage prefix', () => {
    template.hasResourceProperties('AWS::ApiGateway::RestApi', {
      Name: 'alpha-TrunkfulApi',
    });
  });

  test('creates Cognito User Pool', () => {
    template.resourceCountIs('AWS::Cognito::UserPool', 1);
  });

  test('creates WAF WebACL', () => {
    template.resourceCountIs('AWS::WAFv2::WebACL', 1);
  });

  test('creates IoT Topic Rule', () => {
    template.hasResourceProperties('AWS::IoT::TopicRule', {
      TopicRulePayload: {
        Sql: "SELECT *, topic(2) as deviceId FROM 'pos/+/orders'",
      },
    });
  });

  test('creates S3 upload bucket', () => {
    // At least 2 S3 buckets: upload + data lake (+ auto-delete custom resource buckets)
    const buckets = template.findResources('AWS::S3::Bucket');
    expect(Object.keys(buckets).length).toBeGreaterThanOrEqual(2);
  });

  test('creates Firehose delivery stream with stage prefix', () => {
    template.hasResourceProperties(
      'AWS::KinesisFirehose::DeliveryStream',
      {
        DeliveryStreamName: 'alpha-TrunkfulAnalyticsStream',
      },
    );
  });

  test('creates Glue database and table', () => {
    template.resourceCountIs('AWS::Glue::Database', 1);
    template.resourceCountIs('AWS::Glue::Table', 1);
  });

  test('creates CloudWatch dashboard', () => {
    template.resourceCountIs('AWS::CloudWatch::Dashboard', 1);
  });

  test('creates CloudWatch alarms for DLQs', () => {
    // 5 DLQ alarms + 1 order queue age alarm = 6
    template.resourceCountIs('AWS::CloudWatch::Alarm', 6);
  });

  test('creates Lambda functions', () => {
    // Intake: order, pos, webhook, admin = 4
    // Processing: order-service, inventory, billing, fulfillment, notification = 5
    // + custom resource lambdas from autoDeleteObjects
    const lambdas = template.findResources('AWS::Lambda::Function');
    expect(Object.keys(lambdas).length).toBeGreaterThanOrEqual(9);
  });
});

describe('PipelineStack', () => {
  test('synthesizes without errors', () => {
    const app = new cdk.App();
    const { PipelineStack } = require('../lib/pipeline-stack');
    const stack = new PipelineStack(app, 'TestPipelineStack');
    const template = Template.fromStack(stack);
    expect(template).toBeDefined();
  });
});
