# DEBS 2014 MQTT Replay

CLI Node.js để replay file CSV của DEBS 2014 lên MQTT broker theo đúng đặc tính thời gian trong dataset.

## Cài đặt

```bash
npm install
```

## Chạy

```bash
node src/main.js --file /path/to/debs.csv --topic debs/replay
```

## Tùy chọn

- `--broker <host|url>`: broker MQTT, mặc định `localhost`
- `--port <number>`: port MQTT, mặc định `1883`
- `--topic <topic>`: topic publish
- `--qos <0|1|2>`: MQTT QoS
- `--retain`: bật retain flag
- `--speed-factor <number>`: tăng tốc replay
- `--use-current-time`: thay timestamp trong payload bằng Unix timestamp hiện tại

## Kiến trúc

- `CsvReplayReader`: đọc CSV bằng stream và trả về từng nhóm record cùng timestamp
- `ReplayScheduler`: tính delay giữa các timestamp và sleep
- `MqttPublisher`: kết nối và publish MQTT
- `StatisticsReporter`: in thống kê mỗi giây
- `main`: ghép các thành phần lại với nhau