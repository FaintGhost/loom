package migrate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// The importer never stops services. The operator must stop the Python bot,
// worker, and retrospective timer before invoking it; an idle consumer counts.
func requireOffline(source string) error {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return fmt.Errorf("cannot establish offline source: %w", err)
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		process := filepath.Join("/proc", entry.Name())
		data, err := os.ReadFile(filepath.Join(process, "cmdline"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot inspect process %d for migration: %w", pid, err)
		}
		args := strings.Split(strings.TrimRight(string(data), "\x00"), "\x00")
		var script string
		for _, arg := range args {
			switch filepath.Base(arg) {
			case "bot.py", "worker.py", "retro.py", "fbctl.py":
				script = arg
			}
		}
		if script != "" {
			var selected string
			for i, arg := range args {
				if arg == "--home" && i+1 < len(args) {
					selected = args[i+1]
				}
				if strings.HasPrefix(arg, "--home=") {
					selected = strings.TrimPrefix(arg, "--home=")
				}
			}
			if selected == "" {
				environment, readErr := os.ReadFile(filepath.Join(process, "environ"))
				if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
					return fmt.Errorf("cannot inspect legacy process %d: %w", pid, readErr)
				}
				for pair := range strings.SplitSeq(string(environment), "\x00") {
					if value, ok := strings.CutPrefix(pair, "FB_HOME="); ok {
						selected = value
					}
				}
			}
			cwd, readErr := os.Readlink(filepath.Join(process, "cwd"))
			if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
				return readErr
			}
			if selected == "" {
				if !filepath.IsAbs(script) {
					script = filepath.Join(cwd, script)
				}
				selected = filepath.Dir(script)
			} else if !filepath.IsAbs(selected) {
				selected = filepath.Join(cwd, selected)
			}
			if resolved, resolveErr := filepath.EvalSymlinks(selected); resolveErr == nil {
				selected = resolved
			}
			if selected == source {
				return fmt.Errorf("source has a live Python consumer (pid %d); stop bot, worker and retrospective timer before migration", pid)
			}
		}
		descriptors, readErr := os.ReadDir(filepath.Join(process, "fd"))
		if errors.Is(readErr, os.ErrNotExist) {
			continue
		}
		if readErr != nil {
			return fmt.Errorf("cannot inspect process %d source handles: %w", pid, readErr)
		}
		for _, descriptor := range descriptors {
			target, readErr := os.Readlink(filepath.Join(process, "fd", descriptor.Name()))
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			if readErr != nil {
				return readErr
			}
			if target == filepath.Join(source, "store.sqlite") || target == filepath.Join(source, "store.sqlite-wal") {
				return fmt.Errorf("source database is open in process %d; stop all consumers before migration", pid)
			}
		}
	}
	return nil
}
