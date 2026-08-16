package local

import "errors"

var (
	ErrLimitMaxActive = errors.New("can't have any more active packs")
)
