# garbage collection

do one pack at a time

gc struct in local

should all instances GC or just one?
    just one. how does it ensure that there is only one GC job?
        use flock to lock a file in the root of packDir

method can be called:
- ReclaimStorage
- ReclaimSpace
- GarbageCollect
- Deflate
- Compact

packer has a method that accepts 0 args and returns a struct with GC info and an error
the info can be:
- ChunksMovedToNewPack
- PacksDeleted
- NewPacks
- ReclaimedSpaceBytes


## steps


1. get all packs to be GCed. 
    map can be
    ```go
    type ChunkInfoWithID struct {
        Offset  uint64
        Size    uint32
        ChunkID []byte
    }
    type info struct {
        RefCount  uint32
        ShouldGC  bool
        TotalSize uint64
        Chunks    []ChunkInfoWithID
    }
    var GCinfo map[uint64]info
    ```
    - it will first get all packs and their ref count. 
    - pass that to dataStore, which will remove all packs that should NOT be garbage collected
    - pass the updated map to metadataStore which will get all chunks to be garbage collected in each pack

    how it determintes that a pack should GC:
        the dataStore struct has a var which has a threshold, which is the size a pack needs to be below to GC.
        if refcount is len(Chunks) and totalsize is below that threshold it should GC.
        an issue with this is that a pack could be below that threshold even though no chunks have been removed.
            for example if a pack has size 5 and the maxsize is 10, and a chunk is added with size 6, then a 
            new pack will be created. and assuming the threshold is 7, it would always count as a pack to be GCed. 

        
2. once it has all packs that should be GCed, it needs to construct new ones. 
    it will go through each pack, adding chunks until it reaches the threshold. 
    if it used all chunks and it did not reach the threshold of max pack size. then it won't GC

    when a pack is constructed it starts creating it right away. it first makes a new pack in a dir called "migrate"
        the part that gets the last packid on startup, DOESN'T require to look in that new dir
        that is because those packs in migrate only exist during runtime. 
            but then how does it increment the currentPackiD? maybe there should also be a nextPackID?

    it then reads all the chunks into that new pack
    once the pack has been constructed it is moved to completed.
    then it updates the metadata for those packs in one transaction
    then it updates the refcount for all packs

3. goes through all packs, and deletes those that have a refcount of 0


