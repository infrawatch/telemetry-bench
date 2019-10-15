# telemetry-bench

## Description

_telemetry-bench_ works as an AMQP message sender, sending collectd simulated
metrics JSON.

## Building from source

```shell
go get -u github.com/redhat-service-assurance/telemetry-bench/...
cd $GOPATH/src/github.com/redhat-service-assurance/telemetry-bench
dep ensure -v -vendor-only
go build cmd/telemetry-bench.go
```

## Consuming the Docker container

```shell
docker pull quay.io/redhat-service-assurance/telemetry-bench
docker run -it --rm quay.io/redhat-service-assurance/telemetry-bench telemetry-bench
```

## Usage

```shell
usage: ./telemetry-bench (options) ampq://...
options:
    -mode simulate|limit
        Mode:
            simulate: simulate collectd and send metrics
            limit: Limit test to identify how many AMQP messages in a 10 sec.
    -hosts int
            Simulate hosts (default 1)
    -interval int
            Interval (sec) (default 1)
    -metrics int
            Metrics per one AMQP messages (default 1)
    -messages int
            Messages per interval (default 1)
    -send int
            How many metrics sent (default 1, -1 means forever)
    -timepermesgs
            Show verbose messages for each given messages (default -1 = no message)
```

### Example1
```
# Send one json data from one host metric to amqp
$ ./telemetry-bench amqp://localhost:5672/foo
```

### Example2
```
# Simulate sending json data 3 times, each from 2 hosts with 5sec
# intervals, (total 2 * 3 = 6 json messages are sent)
$ ./telemetry-bench -hosts 2 -interval 5 -metrics 1 -send 3 amqp://localhost:5672/foo
```

### Authors
- Tomofumi Hayashi (s1061123)
- (Oct 2019) Chris Sibbitt
