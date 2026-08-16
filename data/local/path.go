package local

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const dirNameCompleted = "completed"
const dirNameActive = "active"

func packName(packID uint64) string {
	return fmt.Sprintf("pack-%d.bin", packID)
}
func (ds *dataStore) completedPackPath(packID uint64) string {
	dir := packID >> 10
	return filepath.Join(ds.packDir, dirNameCompleted, strconv.FormatUint(dir, 10), packName(packID))
}

func (ds *dataStore) activePackPath(packID uint64) string {
	return filepath.Join(ds.packDir, dirNameActive, packName(packID))
}

func (ds *dataStore) activeDir() string {
	return filepath.Join(ds.packDir, dirNameActive)
}

func (ds *dataStore) completedDir() string {
	return filepath.Join(ds.packDir, dirNameCompleted)
}

func packIdFromPath(path string) (uint64, error) {
	s := filepath.Base(path)

	s = strings.TrimPrefix(s, "pack-")
	s = strings.TrimSuffix(s, ".bin")

	return strconv.ParseUint(s, 10, 64)
}
