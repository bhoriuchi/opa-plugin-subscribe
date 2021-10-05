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
