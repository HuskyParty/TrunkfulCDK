import { Construct } from 'constructs';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as iot from 'aws-cdk-lib/aws-iot';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Duration } from 'aws-cdk-lib';
import * as path from 'path';

interface IngestionIotProps {
  stageName: string;
  ordersTable: dynamodb.Table;
  idempotencyTable: dynamodb.Table;
  eventBus: events.EventBus;
}

export class IngestionIotConstruct extends Construct {
  constructor(scope: Construct, id: string, props: IngestionIotProps) {
    super(scope, id);

    // ---------------------------------------------------------------
    // 1. POS Intake Lambda
    // ---------------------------------------------------------------
    const posIntakeFn = new NodejsFunction(this, 'PosIntakeFn', {
      functionName: `${props.stageName}-trunkful-pos-intake`,
      entry: path.join(__dirname, '../../lambda/pos-intake/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
      },
    });

    props.ordersTable.grantReadWriteData(posIntakeFn);
    props.idempotencyTable.grantReadWriteData(posIntakeFn);
    props.eventBus.grantPutEventsTo(posIntakeFn);

    // ---------------------------------------------------------------
    // 2. IoT Topic Rule (L1 — CfnTopicRule)
    // ---------------------------------------------------------------
    const topicRule = new iot.CfnTopicRule(this, 'PosOrderTopicRule', {
      ruleName: `${props.stageName}-TrunkfulPosOrderRule`,
      topicRulePayload: {
        sql: "SELECT *, topic(2) as deviceId FROM 'pos/+/orders'",
        awsIotSqlVersion: '2016-03-23',
        ruleDisabled: false,
        actions: [
          {
            lambda: {
              functionArn: posIntakeFn.functionArn,
            },
          },
        ],
      },
    });

    // ---------------------------------------------------------------
    // 3. Lambda Permission for IoT to invoke
    // ---------------------------------------------------------------
    posIntakeFn.addPermission('IoTInvoke', {
      principal: new iam.ServicePrincipal('iot.amazonaws.com'),
      sourceArn: topicRule.attrArn,
    });

    // ---------------------------------------------------------------
    // 4. IoT Policy template for POS device permissions
    // ---------------------------------------------------------------
    new iot.CfnPolicy(this, 'PosDevicePolicy', {
      policyName: `${props.stageName}-TrunkfulPosDevicePolicy`,
      policyDocument: {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Action: ['iot:Connect'],
            Resource: ['*'],
          },
          {
            Effect: 'Allow',
            Action: ['iot:Publish'],
            Resource: ['arn:aws:iot:*:*:topic/pos/*/orders'],
          },
        ],
      },
    });
  }
}
