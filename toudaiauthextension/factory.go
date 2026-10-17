package toudaiauthextension

import (
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

var componentType = component.MustNewType("toudaiauth")

// NewFactory returns the extension factory.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		componentType,
		func() component.Config { return defaultConfig() },
		createExtension,
		component.StabilityLevelDevelopment,
	)
}
