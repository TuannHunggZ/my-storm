const mqtt = require('mqtt');

class MqttPublisher {
  constructor({ broker, port, topic, qos = 0, retain = false }) {
    if (!broker) {
      throw new Error('broker is required');
    }

    if (!topic) {
      throw new Error('topic is required');
    }

    this.broker = broker;
    this.port = port;
    this.topic = topic;
    this.qos = qos;
    this.retain = retain;
    this.client = null;
  }

  async connect() {
    if (this.client) {
      return;
    }

    const connectUrl = /^mqtts?:\/\//i.test(this.broker) ? this.broker : `mqtt://${this.broker}`;

    this.client = mqtt.connect(connectUrl, {
      port: this.port
    });

    await new Promise((resolve, reject) => {
      const onConnect = () => {
        this.client.removeListener('error', onError);
        resolve();
      };

      const onError = (error) => {
        this.client.removeListener('connect', onConnect);
        reject(error);
      };

      this.client.once('connect', onConnect);
      this.client.once('error', onError);
    });
  }

  publish(payload) {
    if (!this.client) {
      throw new Error('MQTT client is not connected');
    }

    const message = typeof payload === 'string' || Buffer.isBuffer(payload)
      ? payload
      : JSON.stringify(payload);

    return new Promise((resolve, reject) => {
      this.client.publish(this.topic, message, { qos: this.qos, retain: this.retain }, (error) => {
        if (error) {
          reject(error);
          return;
        }

        resolve();
      });
    });
  }

  async close() {
    if (!this.client) {
      return;
    }

    await new Promise((resolve) => {
      this.client.end(false, {}, resolve);
    });

    this.client = null;
  }
}

module.exports = MqttPublisher;