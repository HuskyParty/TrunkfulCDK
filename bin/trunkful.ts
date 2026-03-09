#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { PipelineStack } from '../lib/pipeline-stack';
import { TrunkfulStack } from '../lib/trunkful-stack';
import { ALPHA_CONFIG } from '../lib/stage-config';

const app = new cdk.App();

// When LOCAL_DEV=true, deploy a standalone stack (no pipeline) for local iteration
if (process.env.LOCAL_DEV === 'true') {
  new TrunkfulStack(app, 'TrunkfulStack', {
    stageConfig: ALPHA_CONFIG,
    description: 'Trunkful Local Dev Stack',
  });
} else {
  new PipelineStack(app, 'TrunkfulPipelineStack', {
    description: 'Trunkful CI/CD Pipeline with Alpha and Prod stages',
  });
}
