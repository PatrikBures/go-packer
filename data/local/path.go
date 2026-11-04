package data

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
	dir := ds.completedPackDirPath(packID)
	return filepath.Join(dir, packName(packID))
}

func (ds *dataStore) completedPackDirPath(packID uint64) string {
	dirID := ds.completedPackDirID(packID)
	return filepath.Join(ds.packDir, dirNameCompleted, strconv.FormatUint(dirID, 10))
}

func (ds *dataStore) completedPackDirID(packID uint64) uint64 {
	return packID >> 10
}

func (ds *dataStore) activePackPath(packID uint64) string {
	return filepath.Join(ds.packDir, dirNameActive, packName(packID))
}

func (ds *dataStore) activePackLockPath(packID uint64) string {
	return ds.activePackPath(packID) + ".lock"
}

func (ds *dataStore) completedDir() string {
	return filepath.Join(ds.packDir, dirNameCompleted)
}
func (ds *dataStore) activeDir() string {
	return filepath.Join(ds.packDir, dirNameActive)
}

func packIdFromPath(path string) (uint64, error) {
	s := filepath.Base(path)

	s = strings.TrimPrefix(s, "pack-")
	s = strings.TrimSuffix(s, ".bin")

	return strconv.ParseUint(s, 10, 64)
}
