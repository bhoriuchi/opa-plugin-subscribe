# opa-plugin-subscribe

Trigger plugins on events. This plugin allows OPA to subscribe to topics and trigger plugin updates when messages are received. This may be useful for subscribing to bundle updates and pulling them in real-time as they are updated.

The plugin provides an entry point and plugable subscriber providers. This allows the developer to only include the providers needed and keep the compiled OPA CLI lean.

## Installation

```sh
$ go get -u github.com/bhoriuchi/opa-plugin-subscribe
```

## Basic Usage
```go
package main

import (
	"fmt"
	"os"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	"github.com/open-policy-agent/opa/cmd"
	"github.com/open-policy-agent/opa/runtime"

	// import subscriber providers to make them available
	// this limits code dependencies to only the subscribers you need
  _ "github.com/bhoriuchi/opa-plugin-subscribe/subscribe/consul"
	_ "github.com/bhoriuchi/opa-plugin-subscribe/subscribe/kafka"
	_ "github.com/bhoriuchi/opa-plugin-subscribe/subscribe/nats"
)

func main() {
	runtime.RegisterPlugin(subscribe.PluginName, subscribe.Factory{})

	if err := cmd.RootCommand.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

```

## Build
```sh
$ go build -o opa
```

## Providers

### Kafka

#### Kafka Config Example

```yaml
plugins:
  subscribe:
      kafka_bundle:
        provider: kafka
        topic: bundle-update
        plugin: bundle
        config:
          group_id: opa-{{uuid}}
          cleanup_group: true
          version: "2.7.0"
          brokers:
            - kafka:19092
```
#### Publishing Kafka Events

Using the [kaf CLI](https://github.com/birdayz/kaf)

```sh
$ echo test | kaf produce bundle-update
```

### NATS

#### NATS Config Example

```yaml
plugins:
  subscribe:
    subscribers:
      nats_bundle:
        provider: nats
        topic: bundle-update
        plugin: bundle
        config:
          servers:
            - nats://nats:4222
```
#### Publishing NATS Events

Using the [NATS CLI](https://github.com/nats-io/natscli)

```sh
$ nats pub bundle-update test
```

### Consul

#### Consul Config Example

```yaml
plugins:
  subscribe:
    subscribers:
      consul_bundle:
        provider: consul
        topic: bundle-update
        plugin: bundle
        config:
          address: http://consul:8500
```
#### Publishing Consul Events

Using the [Fire Event API](https://www.consul.io/api-docs/event#fire-event)

```sh
$ curl \
    --request PUT \
    --data @payload \
    http://consul:8500/v1/event/fire/bundle-update
```