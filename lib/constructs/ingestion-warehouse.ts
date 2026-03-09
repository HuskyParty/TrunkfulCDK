import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Stack } from 'aws-cdk-lib';

interface IngestionWarehouseProps {
  stageName: string;
  eventBus: events.EventBus;
}

export class IngestionWarehouseConstruct extends Construct {
  public readonly warehouseRole: iam.Role;

  constructor(scope: Construct, id: string, props: IngestionWarehouseProps) {
    super(scope, id);

    // ---------------------------------------------------------------
    // IAM Role for warehouse scanners to put events directly to EventBridge
    // ---------------------------------------------------------------
    this.warehouseRole = new iam.Role(this, 'WarehouseScannerRole', {
      roleName: `${props.stageName}-TrunkfulWarehouseScannerRole`,
      description:
        'Assumed by warehouse scanner devices to publish inventory events to EventBridge',
      assumedBy: new iam.AccountPrincipal(Stack.of(this).account),
    });

    // Grant the role permission to put events on the bus
    props.eventBus.grantPutEventsTo(this.warehouseRole);

    // Add a scoped policy restricting PutEvents to source = 'trunkful.warehouse'
    this.warehouseRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['events:PutEvents'],
        resources: [props.eventBus.eventBusArn],
        conditions: {
          StringEquals: {
            'events:source': 'trunkful.warehouse',
          },
        },
      }),
    );
  }
}
