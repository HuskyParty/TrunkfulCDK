import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { TrunkfulStack } from './trunkful-stack';
import { StageConfig } from './stage-config';

export interface TrunkfulStageProps extends cdk.StageProps {
  stageConfig: StageConfig;
}

export class TrunkfulStage extends cdk.Stage {
  constructor(scope: Construct, id: string, props: TrunkfulStageProps) {
    super(scope, id, props);

    new TrunkfulStack(this, 'TrunkfulStack', {
      stageConfig: props.stageConfig,
      description: `Trunkful Order Processing (${props.stageConfig.stageName})`,
    });
  }
}
