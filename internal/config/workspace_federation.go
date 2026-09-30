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
const FederationPrefixHomeKey = federationPrefixHomeNamespace + "."

const federationPrefixHomeNamespace = "federation.prefix_home"

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
// case-variant value that this reader, and so the wake, ignores. Within one
// file a prefix home must be declared once: two yaml paths that produce the
// same key would otherwise resolve in map order, differently on each replica.
func ReadWorkspaceFederation(beadsDir string) (WorkspaceFederation, error) {
	var out WorkspaceFederation
	leaves := map[string]interface{}{}
	for _, name := range []string{"config.yaml", "config.local.yaml"} {
		file, err := flattenYamlFile(filepath.Join(beadsDir, name))
		if err != nil {
			return out, err
		}
		for key, value := range file { // config.local.yaml over config.yaml
			leaves[key] = value
		}
	}
	if _, ok := leaves[federationPrefixHomeNamespace]; ok {
		return out, fmt.Errorf("%s in %s must map prefixes to nodes", federationPrefixHomeNamespace, beadsDir)
	}
	homes := map[string]string{}
	for key, value := range leaves {
		prefix, ok := strings.CutPrefix(key, FederationPrefixHomeKey)
		if !ok {
			continue
		}
		// A bead prefix holds no dot (dots mark child ids), so a dotted
		// remainder is a mapping where a node name belongs.
		home, isString := value.(string)
		if home = strings.TrimSpace(home); prefix == "" || strings.Contains(prefix, ".") || !isString || home == "" {
			return out, fmt.Errorf("%s in %s does not name one node for one prefix", key, beadsDir)
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

// flattenYamlFile returns one yaml file's leaves under dotted keys. A missing
// file has none; a file that cannot be read or parsed is an error.
func flattenYamlFile(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path) //nolint:gosec // path is a caller-resolved workspace config file
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var root map[string]interface{}
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	leaves := map[string]interface{}{}
	if err := flattenYaml(root, "", leaves); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return leaves, nil
}

// flattenYaml walks a decoded yaml mapping into dst under dotted keys. An
// empty mapping is kept as a leaf, so a declaration with no usable value is
// seen rather than dropped. A federation.prefix_home key that two yaml paths
// of the one file both produce is an error: which one won would depend on
// map order.
func flattenYaml(node map[string]interface{}, path string, dst map[string]interface{}) error {
	for key, value := range node {
		full := key
		if path != "" {
			full = path + "." + key
		}
		if child, ok := value.(map[string]interface{}); ok && len(child) > 0 {
			if err := flattenYaml(child, full, dst); err != nil {
				return err
			}
			continue
		}
		if _, twice := dst[full]; twice && strings.HasPrefix(full, federationPrefixHomeNamespace) {
			return fmt.Errorf("%s is declared twice", full)
		}
		dst[full] = value
	}
	return nil
}
