/**
 * DynamoDB-backed circuit breaker for protecting external service calls.
 *
 * Stores state in the Orders table using a CIRCUIT#{serviceName} key pattern.
 * States: CLOSED (healthy), OPEN (tripped), HALF_OPEN (testing recovery).
 */
import {
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb';

const ddb = new DynamoDBClient({});
const TABLE = process.env.ORDERS_TABLE!;

const FAILURE_THRESHOLD = 5;
const FAILURE_WINDOW_MS = 60_000; // 60 seconds
const COOLDOWN_MS = 30_000; // 30 seconds

type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

interface CircuitRecord {
  state: CircuitState;
  failureCount: number;
  lastFailureTime: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function getCircuitRecord(
  serviceName: string,
): Promise<CircuitRecord | null> {
  const key = `CIRCUIT#${serviceName}`;
  const res = await ddb.send(
    new GetItemCommand({
      TableName: TABLE,
      Key: { pk: { S: key }, sk: { S: key } },
    }),
  );

  if (!res.Item) return null;

  return {
    state: (res.Item.state?.S as CircuitState) ?? 'CLOSED',
    failureCount: Number(res.Item.failureCount?.N ?? '0'),
    lastFailureTime: Number(res.Item.lastFailureTime?.N ?? '0'),
  };
}

async function putCircuitRecord(
  serviceName: string,
  record: CircuitRecord,
  expectedState?: CircuitState,
): Promise<void> {
  const key = `CIRCUIT#${serviceName}`;
  await ddb.send(
    new PutItemCommand({
      TableName: TABLE,
      Item: {
        pk: { S: key },
        sk: { S: key },
        state: { S: record.state },
        failureCount: { N: String(record.failureCount) },
        lastFailureTime: { N: String(record.lastFailureTime) },
      },
      // Ensure atomic state transitions: only write if the record does not
      // yet exist OR the current state matches the caller's expected state.
      ConditionExpression: 'attribute_not_exists(pk) OR #state = :expectedState',
      ExpressionAttributeNames: {
        '#state': 'state',
      },
      ExpressionAttributeValues: {
        ':expectedState': { S: expectedState ?? record.state },
      },
    }),
  );
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Check the current state of the circuit breaker for the given service.
 * If the circuit is OPEN and the cooldown has elapsed it transitions to
 * HALF_OPEN so the next call can be attempted as a probe.
 */
export async function checkCircuit(
  serviceName: string,
): Promise<CircuitState> {
  const record = await getCircuitRecord(serviceName);

  if (!record) return 'CLOSED';

  if (record.state === 'OPEN') {
    const elapsed = Date.now() - record.lastFailureTime;
    if (elapsed >= COOLDOWN_MS) {
      // Transition to HALF_OPEN so a single probe can go through
      await putCircuitRecord(serviceName, { ...record, state: 'HALF_OPEN' }, 'OPEN');
      return 'HALF_OPEN';
    }
    return 'OPEN';
  }

  return record.state;
}

/**
 * Record a successful call. Resets the circuit back to CLOSED.
 */
export async function recordSuccess(serviceName: string): Promise<void> {
  const existing = await getCircuitRecord(serviceName);
  await putCircuitRecord(
    serviceName,
    { state: 'CLOSED', failureCount: 0, lastFailureTime: 0 },
    existing?.state,
  );
}

/**
 * Record a failed call. Opens the circuit after FAILURE_THRESHOLD failures
 * within the configured window.
 */
export async function recordFailure(serviceName: string): Promise<void> {
  const now = Date.now();
  const existing = await getCircuitRecord(serviceName);

  let failureCount = 1;

  if (existing) {
    const withinWindow = now - existing.lastFailureTime < FAILURE_WINDOW_MS;
    failureCount = withinWindow ? existing.failureCount + 1 : 1;
  }

  const newState: CircuitState =
    failureCount >= FAILURE_THRESHOLD ? 'OPEN' : 'CLOSED';

  await putCircuitRecord(
    serviceName,
    { state: newState, failureCount, lastFailureTime: now },
    existing?.state,
  );
}
