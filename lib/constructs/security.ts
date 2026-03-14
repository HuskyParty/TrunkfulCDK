import { Construct } from 'constructs';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Duration, RemovalPolicy } from 'aws-cdk-lib';

interface SecurityProps {
  stageName: string;
  removalPolicy: RemovalPolicy;
}

export class SecurityConstruct extends Construct {
  public readonly piiEncryptionKey: kms.Key;
  public readonly paymentApiSecret: secretsmanager.Secret;

  constructor(scope: Construct, id: string, props: SecurityProps) {
    super(scope, id);

    // KMS Customer Managed Key for PII encryption
    this.piiEncryptionKey = new kms.Key(this, 'PiiEncryptionKey', {
      alias: `${props.stageName}-trunkful/pii-encryption`,
      description: `CMK for encrypting PII data (${props.stageName})`,
      enableKeyRotation: true,
      removalPolicy: props.removalPolicy,
    });

    // Secrets Manager secret for payment API key
    this.paymentApiSecret = new secretsmanager.Secret(this, 'PaymentApiSecret', {
      secretName: `${props.stageName}-trunkful/payment-api-key`,
      description: `Payment API key (${props.stageName})`,
      generateSecretString: {
        excludePunctuation: true,
        passwordLength: 32,
      },
    });

    // Rotation Lambda for the payment API secret
    const paymentSecretRotationFn = new lambda.Function(this, 'PaymentSecretRotationFn', {
      functionName: `${props.stageName}-trunkful-payment-secret-rotation`,
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
        exports.handler = async (event) => {
          // Custom rotation logic: retrieve new key from payment provider and update secret
          console.log('Rotating payment API secret', JSON.stringify(event));
        };
      `),
      timeout: Duration.seconds(30),
      description: `Rotates the payment API secret every 90 days (${props.stageName})`,
    });

    // Rotate the payment secret every 90 days
    this.paymentApiSecret.addRotationSchedule('PaymentApiSecretRotation', {
      rotationLambda: paymentSecretRotationFn,
      automaticallyAfter: Duration.days(90),
    });
  }
}
