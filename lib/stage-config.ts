import { RemovalPolicy } from 'aws-cdk-lib';

export interface StageConfig {
  stageName: string;
  removalPolicy: RemovalPolicy;
  autoDeleteObjects: boolean;
  reservedConcurrency: {
    orderService: number;
    inventoryService: number;
    billing: number;
    fulfillment: number;
    notification: number;
  };
}

export const ALPHA_CONFIG: StageConfig = {
  stageName: 'alpha',
  removalPolicy: RemovalPolicy.DESTROY,
  autoDeleteObjects: true,
  reservedConcurrency: {
    orderService: 5,
    inventoryService: 5,
    billing: 2,
    fulfillment: 2,
    notification: 2,
  },
};

export const PROD_CONFIG: StageConfig = {
  stageName: 'prod',
  removalPolicy: RemovalPolicy.RETAIN,
  autoDeleteObjects: false,
  reservedConcurrency: {
    orderService: 50,
    inventoryService: 50,
    billing: 25,
    fulfillment: 25,
    notification: 25,
  },
};
