package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/spf13/cobra"
	"github.com/steveyegge/beads/internal/beads"
	"github.com/steveyegge/beads/internal/metrics"
)

var (
	// Version is the current version of bd (overridden by ldflags at build time)
	Version = "1.1.1"
	// Build can be set via ldflags at compile time
	Build = "dev"
	// Commit and branch the git revision the binary was built from (optional ldflag)
	Commit = ""
	Branch = ""
)

var versionCmd = &cobra.Command{
	Use:           "version",
	Short:         "Print version information",
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		evt := metrics.NewCommandEvent("version")
		defer func() {
			if c := metrics.Global(); c != nil {
				c.CloseEventAndAdd(evt)
			}
		}()

		commit := resolveCommitHash()
		branch := resolveBranch()

		if jsonOutput {
			if err := outputJSON(versionJSONPayload(commit, branch)); err != nil {
				return err
			}
		} else {
			if commit != "" && branch != "" {
				fmt.Printf("bd version %s (%s: %s@%s)\n", Version, Build, branch, shortCommit(commit))
			} else if commit != "" {
				fmt.Printf("bd version %s (%s: %s)\n", Version, Build, shortCommit(commit))
			} else {
				fmt.Printf("bd version %s (%s)\n", Version, Build)
			}
		}

		// Check for multiple bd binaries in PATH
		if dups := findDuplicateBinaries(); len(dups) > 1 {
			fmt.Fprintf(os.Stderr, "\nWarning: multiple 'bd' binaries found in PATH:\n")
			for _, p := range dups {
				fmt.Fprintf(os.Stderr, "  %s\n", p)
			}
			fmt.Fprintf(os.Stderr, "The first one is being used. Remove duplicates to avoid confusion.\n")
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}

// versionJSONPayload builds the `bd version --json` object. The commit key
// is always present, "" when unknown: machine consumers (e.g. the Gas Town
// host comparing its linked beads module pseudo-version suffix) rely on the
// key existing.
func versionJSONPayload(commit, branch string) map[string]interface{} {
	result := map[string]interface{}{
		"version": Version,
		"build":   Build,
		"commit":  commit,
	}
	if branch != "" {
		result["branch"] = branch
	}
	return result
}

func resolveCommitHash() string {
	if Commit != "" {
		return Commit
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				return setting.Value
			}
		}
		// `go install module@version` builds carry no vcs.revision, but a
		// module pseudo-version pins the commit in its suffix.
		if commit := commitFromModuleVersion(info.Main.Version); commit != "" {
			return commit
		}
	}

	return ""
}

// commitFromModuleVersion extracts the 12-char commit hash from a Go module
// pseudo-version (e.g. v1.1.0-rc.2.0.20260702201530-193a87707245). Returns ""
// for release tags, plain pre-releases, "(devel)", and anything else that is
// not a pseudo-version.
func commitFromModuleVersion(version string) string {
	// Strip build metadata such as +incompatible.
	if i := strings.IndexByte(version, '+'); i >= 0 {
		version = version[:i]
	}

	parts := strings.Split(version, "-")
	if len(parts) < 3 || !strings.HasPrefix(parts[0], "v") {
		return ""
	}

	rev := parts[len(parts)-1]
	if len(rev) != 12 || !isLowerHex(rev) {
		return ""
	}

	// The segment before the revision must end in the 14-digit UTC
	// timestamp (yyyymmddhhmmss) that pseudo-versions carry; otherwise this
	// is an ordinary pre-release that happens to end in 12 hex chars.
	timestamp := parts[len(parts)-2]
	if i := strings.LastIndexByte(timestamp, '.'); i >= 0 {
		timestamp = timestamp[i+1:]
	}
	if len(timestamp) != 14 || !isDigits(timestamp) {
		return ""
	}

	return rev
}

func isLowerHex(s string) bool {
	for _, r := range s {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func isDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func shortCommit(hash string) string {
	if len(hash) > 12 {
		return hash[:12]
	}
	return hash
}

func resolveBranch() string {
	if Branch != "" {
		return Branch
	}

	// Try to get branch from build info (build-time VCS detection)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.branch" && setting.Value != "" {
				return setting.Value
			}
		}
	}

	// Fallback: try to get branch from git at runtime
	// Use symbolic-ref to work in fresh repos without commits
	// Uses CWD repo context since this shows user's current branch
	if rc, err := beads.GetRepoContext(); err == nil {
		cmd := rc.GitCmdCWD(context.Background(), "symbolic-ref", "--short", "HEAD")
		if output, err := cmd.Output(); err == nil {
			if branch := strings.TrimSpace(string(output)); branch != "" && branch != "HEAD" {
				return branch
			}
		}
	}

	return ""
}

// findDuplicateBinaries searches PATH for all "bd" executables.
// Returns their full paths. If len > 1, there are duplicates.
func findDuplicateBinaries() []string {
	name := "bd"
	if runtime.GOOS == "windows" {
		name = "bd.exe"
	}

	seen := make(map[string]bool)
	var paths []string

	for _, dir := range filepath.SplitList(os.Getenv("PATH")) {
		candidate := filepath.Join(dir, name)
		// Resolve symlinks so we don't double-count
		resolved, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			// Try the raw path (might be a valid binary without symlinks)
			resolved = candidate
		}
		info, err := os.Stat(candidate)
		if err != nil {
			continue
		}
		if info.IsDir() {
			continue
		}
		if !seen[resolved] {
			seen[resolved] = true
			paths = append(paths, candidate)
		}
	}
	return paths
}
