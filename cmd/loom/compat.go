package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/FaintGhost/loom/internal/improvement"
	"github.com/FaintGhost/loom/internal/worker"
)

func runCompatibility(ctx context.Context, home string, cfg worker.Config, output io.Writer) error {
	folder, err := improvement.CheckCompatibility(ctx, home, cfg)
	if folder != "" {
		err = errors.Join(err, json.NewEncoder(output).Encode(map[string]string{"evidence": folder}))
	}
	return err
}
