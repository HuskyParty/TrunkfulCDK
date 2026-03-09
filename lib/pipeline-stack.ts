import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as pipelines from 'aws-cdk-lib/pipelines';
import { TrunkfulStage } from './trunkful-stage';
import { ALPHA_CONFIG, PROD_CONFIG } from './stage-config';

export class PipelineStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // ---------------------------------------------------------------
    // 1. CDK Pipeline — self-mutating
    // ---------------------------------------------------------------
    const pipeline = new pipelines.CodePipeline(this, 'TrunkfulPipeline', {
      pipelineName: 'TrunkfulPipeline',
      synth: new pipelines.ShellStep('Synth', {
        input: pipelines.CodePipelineSource.connection(
          'OWNER/REPO', // Replace with your GitHub org/repo
          'main',
          {
            connectionArn:
              'arn:aws:codeconnections:us-west-2:ACCOUNT:connection/CONNECTION_ID', // Replace with your CodeStar connection ARN
          },
        ),
        commands: [
          'npm ci',
          'npx cdk synth',
        ],
      }),
      dockerEnabledForSynth: true,
    });

    // ---------------------------------------------------------------
    // 2. Alpha Stage — auto-deploy on every push
    // ---------------------------------------------------------------
    const alphaStage = new TrunkfulStage(this, 'Alpha', {
      stageConfig: ALPHA_CONFIG,
    });
    pipeline.addStage(alphaStage);

    // ---------------------------------------------------------------
    // 3. Prod Stage — manual approval gate
    // ---------------------------------------------------------------
    const prodStage = new TrunkfulStage(this, 'Prod', {
      stageConfig: PROD_CONFIG,
    });
    pipeline.addStage(prodStage, {
      pre: [
        new pipelines.ManualApprovalStep('PromoteToProd', {
          comment: 'Review Alpha deployment and approve promotion to Production.',
        }),
      ],
    });
  }
}
