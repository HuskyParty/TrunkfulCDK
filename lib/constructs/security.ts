import { Construct } from 'constructs';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { RemovalPolicy } from 'aws-cdk-lib';

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
  }
}
