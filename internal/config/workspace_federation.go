package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// FederationPrefixHomeKey is the config namespace that names the home node of
// a bead prefix in a federated store: federation.prefix_home.hw: citadel.
const FederationPrefixHomeKey = "federation.prefix_home."

// WorkspaceFederation is what the defer-wake owner scope reads for ONE store.
type WorkspaceFederation struct {
	// PrefixHomes maps a bead prefix to its home node. Empty means the store
	// declares no federation.
	PrefixHomes map[string]string
	// NodeID names this node FOR THE WAKE, resolved only when the store is
	// federated: BEADS_NODE_ID / BD_NODE_ID, else node_id in the store's own
	// files, else the user-level files (documented over native over legacy) —
	// the precedence bd's own process gives node_id. It is deliberately not
	// NodeID(): that function and the lease guard behind it stay as they are.
	NodeID string
}

// ReadWorkspaceFederation reads ONE store's federation declaration from its
// own files — config.yaml with config.local.yaml over it, the pair and the
// precedence Initialize gives a workspace — without touching the process-wide
// viper state, the environment or the user-level config. It keeps three
// outcomes apart: neither file declares a federation.prefix_home.<prefix> key
// (empty PrefixHomes, nil error); the keys are present and usable; or a file
// cannot be read or parsed, or a key has no usable value (an error). A caller
// must not read the last one as "not federated".
//
// Keys are collected from the flat form, the nested form and any mix of the
// two, and are case-exact: `bd config get federation.prefix_home.<prefix>`
// answers from viper's merged view and can show a user-level, environment or
// case-variant value that this reader, and so the wake, ignores.
func ReadWorkspaceFederation(beadsDir string) (WorkspaceFederation, error) {
	var out WorkspaceFederation
	leaves := map[string]interface{}{}
	for _, name := range []string{"config.yaml", "config.local.yaml"} {
		if err := flattenYamlFile(filepath.Join(beadsDir, name), leaves); err != nil {
			return out, err
		}
	}
	if _, ok := leaves[strings.TrimSuffix(FederationPrefixHomeKey, ".")]; ok {
		return out, fmt.Errorf("%s in %s must map prefixes to nodes", strings.TrimSuffix(FederationPrefixHomeKey, "."), beadsDir)
	}
	homes := map[string]string{}
	for key, value := range leaves {
		prefix, ok := strings.CutPrefix(key, FederationPrefixHomeKey)
		if !ok || prefix == "" {
			continue
		}
		home, ok := value.(string)
		if home = strings.TrimSpace(home); !ok || home == "" {
			return out, fmt.Errorf("%s in %s names no node", key, beadsDir)
		}
		homes[prefix] = home
	}
	if len(homes) == 0 {
		return out, nil
	}
	out.PrefixHomes = homes
	out.NodeID = federationNodeID(leaves)
	return out, nil
}

// federationNodeID resolves this node for a federated store's wake.
func federationNodeID(storeLeaves map[string]interface{}) string {
	for _, name := range []string{"BEADS_NODE_ID", "BD_NODE_ID"} {
		if node := strings.TrimSpace(os.Getenv(name)); node != "" {
			return node
		}
	}
	if node, ok := storeLeaves["node_id"].(string); ok && strings.TrimSpace(node) != "" {
		return strings.TrimSpace(node)
	}
	candidates := currentUserConfigYamlCandidates()
	for _, path := range []string{candidates.documented, candidates.native, candidates.legacy} {
		if path == "" {
			continue
		}
		if node, ok := readYamlValueAtPath(path, "node_id"); ok && strings.TrimSpace(node) != "" {
			return strings.TrimSpace(node)
		}
	}
	return ""
}

// flattenYamlFile merges one yaml file's leaves into dst under dotted keys,
// later calls overriding earlier ones. A missing file adds nothing; a file
// that cannot be read or parsed is an error.
func flattenYamlFile(path string, dst map[string]interface{}) error {
	data, err := os.ReadFile(path) //nolint:gosec // path is a caller-resolved workspace config file
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	var root map[string]interface{}
	if err := yaml.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	flattenYaml(root, "", dst)
	return nil
}

func flattenYaml(node map[string]interface{}, path string, dst map[string]interface{}) {
	for key, value := range node {
		full := key
		if path != "" {
			full = path + "." + key
		}
		if child, ok := value.(map[string]interface{}); ok {
			flattenYaml(child, full, dst)
			continue
		}
		dst[full] = value
	}
}
