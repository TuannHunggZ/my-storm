const { sleep } = require('./ReplayScheduler');

class StatisticsReporter {
  constructor({ logger = console.log } = {}) {
    this.logger = logger;
    this.totalMessages = 0;
    this.messagesSinceLastReport = 0;
    this.currentDatasetTimestamp = null;
    this.startedAt = null;
    this.stopped = true;
    this.reportTask = null;
  }

  start() {
    if (!this.stopped) {
      return;
    }

    this.stopped = false;
    this.startedAt = Date.now();
    this.reportTask = this.run();
  }

  stop() {
    this.stopped = true;
  }

  markMessage(timestamp, count = 1) {
    this.totalMessages += count;
    this.messagesSinceLastReport += count;
    this.currentDatasetTimestamp = timestamp;
  }

  async run() {
    while (!this.stopped) {
      await sleep(1000);

      if (this.stopped) {
        break;
      }

      const elapsedMs = Date.now() - this.startedAt;
      const elapsedSeconds = Math.max(elapsedMs / 1000, 1);
      const messagesPerSecond = this.messagesSinceLastReport;
      const currentDatasetTimestamp = this.currentDatasetTimestamp === null ? 'N/A' : this.currentDatasetTimestamp;

      this.logger(
        `[stats] sent=${this.messagesSinceLastReport} msg/s=${messagesPerSecond} datasetTimestamp=${currentDatasetTimestamp} total=${this.totalMessages} elapsedMs=${elapsedMs}`
      );

      this.messagesSinceLastReport = 0;
    }
  }

  async waitForStop() {
    if (this.reportTask) {
      await this.reportTask;
    }
  }
}

module.exports = StatisticsReporter;