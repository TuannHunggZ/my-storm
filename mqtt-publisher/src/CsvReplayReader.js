const fs = require('node:fs');
const readline = require('node:readline');

function parseCsvLine(line) {
  const columns = line.split(',');

  if (columns.length < 7) {
    throw new Error(`Invalid CSV line, expected 7 columns: ${line}`);
  }

  const [id, timestamp, value, property, plugId, householdId, houseId] = columns;

  return {
    id: Number(id),
    timestamp: Number(timestamp),
    value,
    property,
    plug_id: Number(plugId),
    household_id: Number(householdId),
    house_id: Number(houseId)
  };
}

class CsvReplayReader {
  constructor(filePath) {
    this.filePath = filePath;
  }

  async *readGroups() {
    const stream = fs.createReadStream(this.filePath, { encoding: 'utf8' });
    const lineReader = readline.createInterface({
      input: stream,
      crlfDelay: Infinity
    });

    let currentTimestamp = null;
    let currentGroup = [];

    try {
      for await (const rawLine of lineReader) {
        const line = rawLine.trim();

        if (!line) {
          continue;
        }

        const record = parseCsvLine(line);

        if (currentTimestamp === null) {
          currentTimestamp = record.timestamp;
        }

        if (record.timestamp !== currentTimestamp) {
          yield {
            timestamp: currentTimestamp,
            records: currentGroup
          };

          currentTimestamp = record.timestamp;
          currentGroup = [];
        }

        currentGroup.push(record);
      }

      if (currentGroup.length > 0) {
        yield {
          timestamp: currentTimestamp,
          records: currentGroup
        };
      }
    } finally {
      lineReader.close();
      stream.destroy();
    }
  }
}

module.exports = CsvReplayReader;
module.exports.parseCsvLine = parseCsvLine;