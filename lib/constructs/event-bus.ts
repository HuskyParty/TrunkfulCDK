import { Construct } from 'constructs';
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

    // Archive all order events for 90 days for replay / auditing
    this.bus.archive('TrunkfulArchive', {
      archiveName: `${props.stageName}-TrunkfulArchive`,
      description: `Archive of Trunkful order events (${props.stageName})`,
      eventPattern: {
        source: ['trunkful.orders'],
      },
      retention: Duration.days(90),
    });
  }
}
