const path = require('node:path');
const CsvReplayReader = require('./CsvReplayReader');
const ReplayScheduler = require('./ReplayScheduler');
const MqttPublisher = require('./MqttPublisher');
const StatisticsReporter = require('./StatisticsReporter');

function printUsage() {
  console.log(`Usage: node src/main.js --file <csv> --topic <topic> [options]

Required:
  --file <path>             Path to the DEBS 2014 CSV file
  --topic <topic>           MQTT topic to publish to

MQTT:
  --broker <host|url>       MQTT broker host or URL (default: localhost)
  --port <number>           MQTT broker port (default: 1883)
  --qos <0|1|2>             MQTT QoS (default: 0)
  --retain                  Set MQTT retain flag

Replay:
  --speed-factor <number>   Replay speed factor (default: 1)
  --use-current-time        Replace timestamp in payload with current Unix timestamp

Examples:
  node src/main.js --file data.csv --topic debs/replay
  node src/main.js --file data.csv --topic debs/replay --broker localhost --port 1883 --speed-factor 10
  node src/main.js --file data.csv --topic debs/replay --use-current-time`);
}

function parseArgs(argv) {
  const options = {
    broker: 'localhost',
    port: 1883,
    qos: 0,
    retain: false,
    speedFactor: 1,
    useCurrentTime: false
  };

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];

    switch (arg) {
      case '--file':
        options.file = argv[++index];
        break;
      case '--broker':
        options.broker = argv[++index];
        break;
      case '--port':
        options.port = Number(argv[++index]);
        break;
      case '--topic':
        options.topic = argv[++index];
        break;
      case '--qos':
        options.qos = Number(argv[++index]);
        break;
      case '--retain':
        options.retain = true;
        break;
      case '--speed-factor':
        options.speedFactor = Number(argv[++index]);
        break;
      case '--use-current-time':
        options.useCurrentTime = true;
        break;
      case '--help':
      case '-h':
        options.help = true;
        break;
      default:
        throw new Error(`Unknown argument: ${arg}`);
    }
  }

  return options;
}

function buildPayload(record, useCurrentTime) {
  if (!useCurrentTime) {
    return record;
  }

  return {
    ...record,
    timestamp: Math.floor(Date.now() / 1000)
  };
}

async function main() {
  let options;

  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(error.message);
    printUsage();
    process.exitCode = 1;
    return;
  }

  if (options.help || !options.file || !options.topic) {
    printUsage();
    if (!options.help) {
      process.exitCode = 1;
    }
    return;
  }

  if (!Number.isFinite(options.port) || !Number.isFinite(options.qos) || !Number.isFinite(options.speedFactor)) {
    throw new Error('port, qos, and speedFactor must be valid numbers');
  }

  const csvPath = path.resolve(options.file);
  const reader = new CsvReplayReader(csvPath);
  const scheduler = new ReplayScheduler({ speedFactor: options.speedFactor });
  const publisher = new MqttPublisher({
    broker: options.broker,
    port: options.port,
    topic: options.topic,
    qos: options.qos,
    retain: options.retain
  });
  const reporter = new StatisticsReporter();

  await publisher.connect();
  reporter.start();

  try {
    let previousTimestamp = null;

    for await (const group of reader.readGroups()) {
      if (previousTimestamp !== null) {
        await scheduler.waitForNext(previousTimestamp, group.timestamp);
      }

      for (const record of group.records) {
        await publisher.publish(buildPayload(record, options.useCurrentTime));
        reporter.markMessage(group.timestamp);
      }

      previousTimestamp = group.timestamp;
    }

    reporter.stop();
    await reporter.waitForStop();
  } catch (error) {
    reporter.stop();
    await reporter.waitForStop();
    throw error;
  } finally {
    await publisher.close();
  }
}

main().catch((error) => {
  console.error(error.stack || error.message || error);
  process.exitCode = 1;
});