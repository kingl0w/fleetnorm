// command genfixtures writes the synthetic MyGeotab FaultData fixtures the
// geotab adapter tests read.
//
// it lives under cmd/ because the go tool ignores every directory named
// testdata: a generator in there would not be built, vetted or compiled by
// `make check`, and would rot with nothing failing. the generation itself is in
// internal/adapter/geotab/fixtures, so the drift test can call it without
// shelling out.
//
// \tgo run ./cmd/genfixtures            # or: make fixtures
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ianfrushon/fleetnorm/internal/adapter/geotab/fixtures"
)

func main() {
	seed := flag.Uint64("seed", fixtures.CommittedSeed, "PRNG seed; the same seed produces byte-identical output")
	count := flag.Int("count", fixtures.CommittedCount, "faults in the ordinary scene")
	out := flag.String("out", "internal/adapter/geotab/testdata", "directory to write the fixtures into")
	flag.Parse()

	if err := run(*seed, *count, *out); err != nil {
		fmt.Fprintln(os.Stderr, "genfixtures:", err)
		os.Exit(1)
	}
}

func run(seed uint64, count int, dir string) error {
	files, err := fixtures.Generate(seed, count)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, f := range files {
		path := filepath.Join(dir, f.Name)
		if err := os.WriteFile(path, f.Data, 0o644); err != nil {
			return err
		}
		fmt.Printf("wrote %s (%d bytes)\n", path, len(f.Data))
	}
	return nil
}
