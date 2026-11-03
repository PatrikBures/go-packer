package packer

import "errors"

var (
	ErrNewPack       = errors.New("requires new pack")
	ErrChunkNotFound = errors.New("chunk not found")
	ErrPackNotFound  = errors.New("pack not found")
	ErrSliceTooSmall = errors.New("provided slice is too small")
)
