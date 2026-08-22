package local

import "errors"

var (
	ErrLimitMaxActive           = errors.New("can't have any more active packs")
	ErrMajorVersionMissmatch    = errors.New("major version missmatch")
	ErrMinorVersion             = errors.New("minor version unsupported")
	ErrHeaderMagic              = errors.New("unexpected magic in header")
	ErrHeaderChecksum           = errors.New("header checksum missmatch")
)
