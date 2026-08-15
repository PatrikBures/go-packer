package metadata

import "errors"

var (
	ErrBucketNotFound = errors.New("bucket not found")
	ErrChunkNotFound = errors.New("chunk not found")
)
