package metadata


func sliceToMapUint64(s []uint64) map[uint64]struct{} {
	m := make(map[uint64]struct{}, len(s))
	for _, i := range s {
		m[i] = struct{}{}
	}
	return m
}

