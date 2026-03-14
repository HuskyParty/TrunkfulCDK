import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import { Duration } from 'aws-cdk-lib';

interface EventBusProps {
  stageName: string;
}

export class EventBusConstruct extends Construct {
  public readonly bus: events.EventBus;

  constructor(scope: Construct, id: string, props: EventBusProps) {
    super(scope, id);

    // Custom EventBridge bus for all Trunkful domain events
    this.bus = new events.EventBus(this, 'TrunkfulEventBus', {
      eventBusName: `${props.stageName}-TrunkfulEventBus`,
    });

    // Archive all events for 90 days for replay / auditing
    this.bus.archive('TrunkfulArchive', {
      archiveName: `${props.stageName}-TrunkfulArchive`,
      description: `Archive of all Trunkful events (${props.stageName})`,
      eventPattern: {
        account: [cdk.Stack.of(this).account],
      },
      retention: Duration.days(90),
    });
  }
}
