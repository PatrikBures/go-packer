package metadata

import (
	"bytes"
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"
	"go.pabu.dev/packer"
)

func (ms *metadataStore) Put(id []byte, ci packer.ChunkInfo) error {
	if err := ms.db.Update(func(tx *bolt.Tx) error {
		buckets, err := openAddChunkBuckets(tx)
		if err != nil {
			return err
		}
		return addChunk(buckets, id, ci)

	}); err != nil {
		return fmt.Errorf("adding chunk: %w", err)
	}

	return nil
}

func (ms *metadataStore) PutChunks(chunks []packer.ChunkInfoWithID) error {
	return ms.db.Update(func(tx *bolt.Tx) error {
		for i, ci := range chunks {
			buckets, err := openAddChunkBuckets(tx)
			if err != nil {
				return err
			}
			if err := addChunk(buckets, ci.Id, ci.ChunkInfo); err != nil {
				return fmt.Errorf("adding chunk %d/%d: %w", i+1, len(chunks), err)
			}
		}
		return nil
	})
}

type addChunkBuckets struct {
	chunks      *bolt.Bucket
	packs       *bolt.Bucket
	packsDelete *bolt.Bucket
}

func openAddChunkBuckets(tx *bolt.Tx) (*addChunkBuckets, error) {
	chunks := tx.Bucket(bucketChunks)
	if chunks == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketChunks)
	}
	packs := tx.Bucket(bucketPacks)
	if packs == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketPacks)
	}
	packsDelete := tx.Bucket(bucketPacksDelete)
	if packsDelete == nil {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, bucketPacksDelete)
	}

	return &addChunkBuckets{
		chunks:      chunks,
		packs:       packs,
		packsDelete: packsDelete,
	}, nil
}

// Puts chunk and puts pack increasing the refcount.
// If the chunk already exists, and its existing packID differs from the new one,
// its refcount is reduced, if the refcount would become negative, that pack is deleted.
//
// it does not open the buckets, as this functions is also used in a loop.
// if it would open the buckets it would then do it each iteration.
func addChunk(bucket *addChunkBuckets, id []byte, ci packer.ChunkInfo) error {

	var packIDKey [8]byte
	binary.LittleEndian.PutUint64(packIDKey[:], ci.PackID)

	existingChunkBytes := bucket.chunks.Get(id)

	if existingChunkBytes != nil {
		if len(existingChunkBytes) != packer.ChunkInfoSize {
			return fmt.Errorf("invalid chunk info, id: %x, len: %d, want len: %d", id, len(existingChunkBytes), packer.ChunkInfoSize)
		}
		if err := addChunkUpdateExistingPack(bucket, ci, existingChunkBytes); err != nil {
			return fmt.Errorf("updating existing pack: %w", err)
		}
	}

	var (
		refsVal [4]byte
		infoVal [packer.ChunkInfoSize]byte
	)

	refsBytes := bucket.packs.Get(packIDKey[:])
	var refs uint32
	if refsBytes != nil {
		if len(refsBytes) != 4 {
			return fmt.Errorf("invalid refcount len %d for pack %d", len(refsBytes), ci.PackID)
		}
		refs = binary.LittleEndian.Uint32(refsBytes)
	}

	binary.LittleEndian.PutUint32(refsVal[:], refs+1)
	ci.PutBytes(infoVal[:])

	if err := bucket.packs.Put(packIDKey[:], refsVal[:]); err != nil {
		return fmt.Errorf("updating pack %d refs: %w", ci.PackID, err)
	}

	if err := bucket.chunks.Put(id, infoVal[:]); err != nil {
		return fmt.Errorf("adding chunk: %w", err)
	}

	return nil
}

func addChunkUpdateExistingPack(bucket *addChunkBuckets, ci packer.ChunkInfo, existingChunkBytes []byte) error {

	existingPackID := binary.LittleEndian.Uint64(existingChunkBytes)

	var want [packer.ChunkInfoSize]byte
	ci.PutBytes(want[:])
	// nothing to do if chunk already exists and packID is same
	if existingPackID == ci.PackID && bytes.Equal(existingChunkBytes, want[:]) {
		return nil
	}

	var existingPackIDKey [8]byte
	binary.LittleEndian.PutUint64(existingPackIDKey[:], existingPackID)

	existingRefsBytesReceived := bucket.packs.Get(existingPackIDKey[:])
	if existingRefsBytesReceived == nil {
		return fmt.Errorf("pack not found: %d", existingPackID)
	}
	if len(existingRefsBytesReceived) != 4 {
		return fmt.Errorf("invalid refcount len %d for pack %d", len(existingRefsBytesReceived), existingPackID)
	}
	existingRefs := binary.LittleEndian.Uint32(existingRefsBytesReceived)

	if err := updateOrDeletePack(bucket, existingRefs, existingPackID, existingPackIDKey); err != nil {
		return fmt.Errorf("updating or deleting pack: %w", err)
	}

	return nil
}
