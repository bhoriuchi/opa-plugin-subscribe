package main

import (
	"fmt"
	"os"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	"github.com/open-policy-agent/opa/cmd"
	"github.com/open-policy-agent/opa/runtime"
)

func main() {
	runtime.RegisterPlugin(subscribe.PluginName, subscribe.Factory{})

	if err := cmd.RootCommand.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
