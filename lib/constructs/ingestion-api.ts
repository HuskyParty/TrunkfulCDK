import { Construct } from 'constructs';
import * as apigateway from 'aws-cdk-lib/aws-apigateway';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as wafv2 from 'aws-cdk-lib/aws-wafv2';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs';
import { Duration } from 'aws-cdk-lib';
import * as path from 'path';

interface IngestionApiProps {
  stageName: string;
  ordersTable: dynamodb.Table;
  idempotencyTable: dynamodb.Table;
  eventBus: events.EventBus;
  piiKey: kms.Key;
}

export class IngestionApiConstruct extends Construct {
  public readonly api: apigateway.RestApi;
  public readonly userPool: cognito.UserPool;

  constructor(scope: Construct, id: string, props: IngestionApiProps) {
    super(scope, id);

    // ---------------------------------------------------------------
    // 1. Cognito User Pool
    // ---------------------------------------------------------------
    this.userPool = new cognito.UserPool(this, 'TrunkfulUserPool', {
      userPoolName: `${props.stageName}-TrunkfulUserPool`,
      selfSignUpEnabled: true,
      signInAliases: { email: true },
      autoVerify: { email: true },
      standardAttributes: {
        email: { required: true, mutable: true },
      },
    });

    const userPoolClient = this.userPool.addClient('TrunkfulApiClient', {
      authFlows: {
        userPassword: true,
        userSrp: true,
      },
    });

    // ---------------------------------------------------------------
    // 2. REST API Gateway
    // ---------------------------------------------------------------
    this.api = new apigateway.RestApi(this, 'TrunkfulApi', {
      restApiName: `${props.stageName}-TrunkfulApi`,
      description: 'Trunkful order processing REST API',
      deployOptions: {
        stageName: 'prod',
        tracingEnabled: true,
        throttlingRateLimit: 1000,
        throttlingBurstLimit: 500,
      },
    });

    const cognitoAuthorizer = new apigateway.CognitoUserPoolsAuthorizer(
      this,
      'CognitoAuthorizer',
      {
        cognitoUserPools: [this.userPool],
        authorizerName: 'TrunkfulCognitoAuthorizer',
      },
    );

    // ---------------------------------------------------------------
    // 3. Order Intake Lambda
    // ---------------------------------------------------------------
    const orderIntakeFn = new NodejsFunction(this, 'OrderIntakeFn', {
      functionName: `${props.stageName}-trunkful-order-intake`,
      entry: path.join(__dirname, '../../lambda/order-intake/index.ts'),
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

    props.ordersTable.grantReadWriteData(orderIntakeFn);
    props.idempotencyTable.grantReadWriteData(orderIntakeFn);
    props.eventBus.grantPutEventsTo(orderIntakeFn);

    // ---------------------------------------------------------------
    // 4. Webhook Intake Lambda
    // ---------------------------------------------------------------

    // Secrets Manager secret for webhook HMAC signing key
    const webhookSecret = new secretsmanager.Secret(this, 'WebhookSecret', {
      secretName: `${props.stageName}-trunkful/webhook-secret`,
      description: `Webhook HMAC signing secret (${props.stageName})`,
      generateSecretString: {
        excludePunctuation: true,
        passwordLength: 32,
      },
    });

    const webhookIntakeFn = new NodejsFunction(this, 'WebhookIntakeFn', {
      functionName: `${props.stageName}-trunkful-webhook-intake`,
      entry: path.join(__dirname, '../../lambda/webhook-intake/index.ts'),
      handler: 'handler',
      runtime: lambda.Runtime.NODEJS_22_X,
      timeout: Duration.seconds(30),
      memorySize: 256,
      tracing: lambda.Tracing.ACTIVE,
      environment: {
        ORDERS_TABLE: props.ordersTable.tableName,
        IDEMPOTENCY_TABLE: props.idempotencyTable.tableName,
        EVENT_BUS_NAME: props.eventBus.eventBusName,
        WEBHOOK_SECRET_ARN: webhookSecret.secretArn,
      },
    });

    webhookSecret.grantRead(webhookIntakeFn);
    props.ordersTable.grantReadWriteData(webhookIntakeFn);
    props.idempotencyTable.grantReadWriteData(webhookIntakeFn);
    props.eventBus.grantPutEventsTo(webhookIntakeFn);

    // ---------------------------------------------------------------
    // 5. Request Schema Validation
    // ---------------------------------------------------------------

    const orderRequestValidator = new apigateway.RequestValidator(
      this,
      'OrderRequestValidator',
      {
        restApi: this.api,
        requestValidatorName: 'OrderRequestBodyValidator',
        validateRequestBody: true,
        validateRequestParameters: false,
      },
    );

    const orderRequestModel = new apigateway.Model(this, 'OrderRequestModel', {
      restApi: this.api,
      modelName: 'OrderRequest',
      contentType: 'application/json',
      description: 'Schema for order request body',
      schema: {
        schema: apigateway.JsonSchemaVersion.DRAFT4,
        title: 'OrderRequest',
        type: apigateway.JsonSchemaType.OBJECT,
        required: ['customerId', 'items'],
        properties: {
          customerId: { type: apigateway.JsonSchemaType.STRING },
          items: {
            type: apigateway.JsonSchemaType.ARRAY,
            minItems: 1,
            items: {
              type: apigateway.JsonSchemaType.OBJECT,
              required: ['sku', 'quantity'],
              properties: {
                sku: { type: apigateway.JsonSchemaType.STRING },
                quantity: {
                  type: apigateway.JsonSchemaType.INTEGER,
                  minimum: 1,
                },
              },
            },
          },
        },
      },
    });

    // ---------------------------------------------------------------
    // 6. API Key + Usage Plan for supplier webhooks
    // ---------------------------------------------------------------

    const supplierApiKey = new apigateway.ApiKey(this, 'SupplierWebhookApiKey', {
      apiKeyName: `${props.stageName}-trunkful-supplier-webhook-key`,
      description: 'API key for supplier webhook authentication',
      enabled: true,
    });

    const webhookUsagePlan = new apigateway.UsagePlan(
      this,
      'SupplierWebhookUsagePlan',
      {
        name: `${props.stageName}-SupplierWebhookUsagePlan`,
        description: 'Usage plan with throttling for supplier webhook endpoints',
        throttle: {
          rateLimit: 100,
          burstLimit: 50,
        },
      },
    );

    webhookUsagePlan.addApiKey(supplierApiKey);

    // ---------------------------------------------------------------
    // 7. API Routes
    // ---------------------------------------------------------------

    // POST /orders — Cognito-authorized order intake
    const ordersResource = this.api.root.addResource('orders');
    ordersResource.addMethod(
      'POST',
      new apigateway.LambdaIntegration(orderIntakeFn),
      {
        authorizer: cognitoAuthorizer,
        authorizationType: apigateway.AuthorizationType.COGNITO,
        requestValidator: orderRequestValidator,
        requestModels: {
          'application/json': orderRequestModel,
        },
      },
    );

    // POST /webhooks/orders — webhook intake (API key required; secret fetched from Secrets Manager in Lambda)
    const webhooksResource = this.api.root.addResource('webhooks');
    const webhooksOrdersResource = webhooksResource.addResource('orders');
    const webhookMethod = webhooksOrdersResource.addMethod(
      'POST',
      new apigateway.LambdaIntegration(webhookIntakeFn),
      {
        authorizationType: apigateway.AuthorizationType.NONE,
        apiKeyRequired: true,
        requestValidator: orderRequestValidator,
        requestModels: {
          'application/json': orderRequestModel,
        },
      },
    );

    webhookUsagePlan.addApiStage({
      stage: this.api.deploymentStage,
      throttle: [
        {
          method: webhookMethod,
          throttle: {
            rateLimit: 100,
            burstLimit: 50,
          },
        },
      ],
    });

    // ---------------------------------------------------------------
    // 8. DynamoDB Direct Integration — GET /orders/{orderId} (CQRS read)
    // ---------------------------------------------------------------
    const orderIdResource = ordersResource.addResource('{orderId}');

    const ddbReadRole = new iam.Role(this, 'ApiGatewayDdbReadRole', {
      assumedBy: new iam.ServicePrincipal('apigateway.amazonaws.com'),
    });

    ddbReadRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['dynamodb:Query'],
        resources: [
          props.ordersTable.tableArn,
          `${props.ordersTable.tableArn}/index/GSI1`,
        ],
      }),
    );

    const ddbIntegration = new apigateway.AwsIntegration({
      service: 'dynamodb',
      action: 'Query',
      options: {
        credentialsRole: ddbReadRole,
        requestTemplates: {
          'application/json': JSON.stringify({
            TableName: props.ordersTable.tableName,
            IndexName: 'GSI1',
            KeyConditionExpression: 'orderId = :orderId',
            ExpressionAttributeValues: {
              ':orderId': { S: "$input.params('orderId')" },
            },
          }),
        },
        integrationResponses: [
          {
            statusCode: '200',
            responseTemplates: {
              'application/json': [
                '#set($items = $input.path(\'$.Items\'))',
                '#if($items.size() == 0)',
                '  {"message": "Order not found"}',
                '#else',
                '  {',
                '    "orders": [',
                '      #foreach($item in $items)',
                '        {',
                '          "orderId": "$item.orderId.S",',
                '          "pk": "$item.pk.S",',
                '          "sk": "$item.sk.S",',
                '          "createdAt": "$item.createdAt.S"',
                '        }#if($foreach.hasNext),#end',
                '      #end',
                '    ]',
                '  }',
                '#end',
              ].join('\n'),
            },
          },
          {
            statusCode: '400',
            selectionPattern: '4\\d{2}',
            responseTemplates: {
              'application/json': '{"message": "Bad request"}',
            },
          },
          {
            statusCode: '500',
            selectionPattern: '5\\d{2}',
            responseTemplates: {
              'application/json': '{"message": "Internal server error"}',
            },
          },
        ],
      },
    });

    orderIdResource.addMethod('GET', ddbIntegration, {
      authorizer: cognitoAuthorizer,
      authorizationType: apigateway.AuthorizationType.COGNITO,
      methodResponses: [
        {
          statusCode: '200',
          responseModels: {
            'application/json': apigateway.Model.EMPTY_MODEL,
          },
        },
        {
          statusCode: '400',
          responseModels: {
            'application/json': apigateway.Model.ERROR_MODEL,
          },
        },
        {
          statusCode: '500',
          responseModels: {
            'application/json': apigateway.Model.ERROR_MODEL,
          },
        },
      ],
    });

    // ---------------------------------------------------------------
    // 9. WAF WebACL
    // ---------------------------------------------------------------
    const webAcl = new wafv2.CfnWebACL(this, 'TrunkfulWaf', {
      defaultAction: { allow: {} },
      scope: 'REGIONAL',
      visibilityConfig: {
        cloudWatchMetricsEnabled: true,
        metricName: 'TrunkfulWafMetrics',
        sampledRequestsEnabled: true,
      },
      rules: [
        {
          name: 'AWSManagedRulesCommonRuleSet',
          priority: 1,
          overrideAction: { none: {} },
          statement: {
            managedRuleGroupStatement: {
              vendorName: 'AWS',
              name: 'AWSManagedRulesCommonRuleSet',
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: 'CommonRuleSetMetric',
            sampledRequestsEnabled: true,
          },
        },
        {
          name: 'AWSManagedRulesSQLiRuleSet',
          priority: 2,
          overrideAction: { none: {} },
          statement: {
            managedRuleGroupStatement: {
              vendorName: 'AWS',
              name: 'AWSManagedRulesSQLiRuleSet',
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: 'SQLiRuleSetMetric',
            sampledRequestsEnabled: true,
          },
        },
        {
          name: 'RateLimitRule',
          priority: 3,
          action: { block: {} },
          statement: {
            rateBasedStatement: {
              limit: 2000,
              aggregateKeyType: 'IP',
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: 'RateLimitMetric',
            sampledRequestsEnabled: true,
          },
        },
        {
          // Block all traffic originating outside allowed countries.
          // Uses a NOT geo-match so requests from non-US countries are blocked.
          name: 'GeoBlockNonUS',
          priority: 4,
          action: { block: {} },
          statement: {
            notStatement: {
              statement: {
                geoMatchStatement: {
                  countryCodes: ['US'],
                },
              },
            },
          },
          visibilityConfig: {
            cloudWatchMetricsEnabled: true,
            metricName: 'GeoBlockNonUSMetric',
            sampledRequestsEnabled: true,
          },
        },
      ],
    });

    // Associate WAF with the API Gateway stage
    new wafv2.CfnWebACLAssociation(this, 'WafApiAssociation', {
      resourceArn: this.api.deploymentStage.stageArn,
      webAclArn: webAcl.attrArn,
    });
  }
}
