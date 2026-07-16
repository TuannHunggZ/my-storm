function sleep(delayMs) {
  return new Promise((resolve) => {
    setTimeout(resolve, delayMs);
  });
}

class ReplayScheduler {
  constructor({ speedFactor = 1 } = {}) {
    if (!Number.isFinite(speedFactor) || speedFactor <= 0) {
      throw new Error('speedFactor must be a positive number');
    }

    this.speedFactor = speedFactor;
  }

  getDelayMs(currentTimestamp, nextTimestamp) {
    if (!Number.isFinite(currentTimestamp) || !Number.isFinite(nextTimestamp)) {
      throw new Error('timestamps must be finite numbers');
    }

    if (nextTimestamp <= currentTimestamp) {
      return 0;
    }

    return Math.max(0, Math.round(((nextTimestamp - currentTimestamp) * 1000) / this.speedFactor));
  }

  async waitForNext(currentTimestamp, nextTimestamp) {
    const delayMs = this.getDelayMs(currentTimestamp, nextTimestamp);

    if (delayMs > 0) {
      await sleep(delayMs);
    }
  }
}

module.exports = ReplayScheduler;
module.exports.sleep = sleep;