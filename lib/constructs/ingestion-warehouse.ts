import { Construct } from 'constructs';
import * as events from 'aws-cdk-lib/aws-events';
import * as iam from 'aws-cdk-lib/aws-iam';

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
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
    });

    // Grant the role permission to put events on the bus
    props.eventBus.grantPutEventsTo(this.warehouseRole);
  }
}
