import { Duration } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';

export const LAMBDA_DEFAULTS = {
  runtime: lambda.Runtime.NODEJS_22_X,
  timeout: Duration.seconds(30),
  memorySize: 256,
  tracing: lambda.Tracing.ACTIVE,
  logRetention: logs.RetentionDays.ONE_MONTH,
};
