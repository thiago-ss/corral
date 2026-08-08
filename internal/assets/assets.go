// Package assets embeds the OpenCode plugin and agent-role config so the
// corral binary is self-contained: `corral init` installs them into a
// project without needing the source tree.
package assets

import _ "embed"

//go:embed corral.ts
var CorralPluginTS string

//go:embed opencode.json
var OpenCodeConfigJSON string
