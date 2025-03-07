package btree

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/sukryu/GoLite/pkg/ports"
)

var _ ports.StoragePort = (*Btree)(nil)

// BtConfig holds configuration for the B-tree.
type BtConfig struct {
	Degree     int
	PageSize   int
	ThreadSafe bool
	CacheSize  int // Max Number of nodes to cache (0 = no caching)
}

// Btree represents a disk-based B-tree.
type Btree struct {
	Degree     int          // Minimum degree (t)
	Length     int          // Total number of items in the tree
	RootOffset int64        // Offset of the root node in the disk file
	file       *os.File     // Disk file handle
	pageSize   int          // Page size in bytes
	nextOffset int64        // Next available offset for new nodes
	mu         sync.RWMutex // Mutex for thread safety
	threadSafe bool         // Flag for thread safety

	// Cache fields
	cache     map[int64]*Node // Offset to Node mapping
	cacheList *list.List      // LRU list for eviction
	cacheSize int             // Max cache capacity
	cacheMu   sync.RWMutex    // Separate mutex for cache operations
}

// Node represents a single node in the B-tree.
type Node struct {
	items           []Item        // Stored key-value pairs
	childrenOffsets []int64       // Offsets of child nodes
	offset          int64         // Disk offset of this node
	elem            *list.Element // LRU list element reference
}

// Item represents a key-value pair with fixed-size fields for optimization.
type Item struct {
	Key   string // Variable-length key (length prefixed)
	Value string // Fixed as string for simplicity (interface{} 대신)
}

func (b *Btree) GetRootOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.RootOffset
}

// GetLength returns the total number of items in the B-tree.
func (b *Btree) GetLength() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Length
}

// GetCacheSize returns the current number of nodes in the cache.
func (b *Btree) GetCacheSize() int {
	b.cacheMu.RLock()
	defer b.cacheMu.RUnlock()
	return b.cacheList.Len()
}

// NewBtree creates a new B-tree instance.
func NewBtree(file *os.File, config BtConfig) *Btree {
	degree := config.Degree
	if degree <= 0 {
		degree = 32
	}
	pageSize := config.PageSize
	if pageSize <= 0 {
		pageSize = 4096 // SQLite 기본값
	}
	cacheSize := config.CacheSize
	if cacheSize < 0 {
		cacheSize = 0 // Disable caching if negative
	}
	b := &Btree{
		Degree:     degree,
		file:       file,
		pageSize:   pageSize,
		RootOffset: 0,
		nextOffset: int64(pageSize),
		threadSafe: config.ThreadSafe,
		cache:      make(map[int64]*Node),
		cacheList:  list.New(),
		cacheSize:  cacheSize,
	}

	// Load metadata from header page (page 0)
	if err := b.loadHeader(); err != nil {
		// If file is new or empty, initialize with default values
		b.saveHeader()
	}
	return b
}

// loadHeader reads the root offset and length from the header page.
func (b *Btree) loadHeader() error {
	data := make([]byte, b.pageSize)
	_, err := b.file.ReadAt(data, 0)        // Header at offset 0
	if err != nil && err.Error() != "EOF" { // Ignore EOF for new files
		return fmt.Errorf("failed to read header: %v", err)
	}
	buf := bytes.NewReader(data)
	var rootOffset int64
	var length int32
	if err := binary.Read(buf, binary.LittleEndian, &rootOffset); err != nil {
		return nil // New file, no header yet
	}
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return nil // Partial header, treat as new
	}
	b.RootOffset = rootOffset
	b.Length = int(length)
	b.nextOffset = int64(b.pageSize) // Reset if needed
	if stat, err := b.file.Stat(); err == nil && stat.Size() > int64(b.pageSize) {
		b.nextOffset = stat.Size() // Use file size for existing data
	}
	return nil
}

// saveHeader writes the root offset and length to the header page.
func (b *Btree) saveHeader() error {
	buf := bytes.NewBuffer(make([]byte, 0, b.pageSize))
	if err := binary.Write(buf, binary.LittleEndian, b.RootOffset); err != nil {
		return fmt.Errorf("failed to write root offset: %v", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, int32(b.Length)); err != nil {
		return fmt.Errorf("failed to write length: %v", err)
	}
	data := buf.Bytes()
	padded := make([]byte, b.pageSize)
	copy(padded, data)
	_, err := b.file.WriteAt(padded, 0)
	if err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}
	return nil
}

// readNodeFromDisk reads a node directly from disk.
func (b *Btree) readNodeFromDisk(offset int64) (*Node, error) {
	data := make([]byte, b.pageSize)
	_, err := b.file.ReadAt(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read node from disk: %v", err)
	}
	buf := bytes.NewReader(data)
	var itemsCount, childrenCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &itemsCount); err != nil {
		return nil, fmt.Errorf("failed to read items count: %v", err)
	}
	if err := binary.Read(buf, binary.LittleEndian, &childrenCount); err != nil {
		return nil, fmt.Errorf("failed to read children count: %v", err)
	}
	n := &Node{offset: offset}
	n.items = make([]Item, itemsCount)
	for i := uint32(0); i < itemsCount; i++ {
		var keyLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to read key length: %v", err)
		}
		keyBytes := make([]byte, keyLen)
		if _, err := buf.Read(keyBytes); err != nil {
			return nil, fmt.Errorf("failed to read key: %v", err)
		}
		var valueLen uint16
		if err := binary.Read(buf, binary.LittleEndian, &valueLen); err != nil {
			return nil, fmt.Errorf("failed to read value length: %v", err)
		}
		valueBytes := make([]byte, valueLen)
		if _, err := buf.Read(valueBytes); err != nil {
			return nil, fmt.Errorf("failed to read value: %v", err)
		}
		n.items[i] = Item{Key: string(keyBytes), Value: string(valueBytes)}
	}
	n.childrenOffsets = make([]int64, childrenCount)
	for i := uint32(0); i < childrenCount; i++ {
		var childOffset int64
		if err := binary.Read(buf, binary.LittleEndian, &childOffset); err != nil {
			return nil, fmt.Errorf("failed to read child offset: %v", err)
		}
		n.childrenOffsets[i] = childOffset
	}
	return n, nil
}

// writeNodeToDisk serializes and writes a node to disk.
func (b *Btree) writeNodeToDisk(n *Node, offset int64) error {
	buf := bytes.NewBuffer(make([]byte, 0, b.pageSize))
	err := binary.Write(buf, binary.LittleEndian, uint32(len(n.items)))
	if err != nil {
		return fmt.Errorf("failed to write items count: %v", err)
	}
	err = binary.Write(buf, binary.LittleEndian, uint32(len(n.childrenOffsets)))
	if err != nil {
		return fmt.Errorf("failed to write children count: %v", err)
	}
	for _, item := range n.items {
		keyLen := uint16(len(item.Key))
		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			return fmt.Errorf("failed to write key length: %v", err)
		}
		if _, err := buf.WriteString(item.Key); err != nil {
			return fmt.Errorf("failed to write key: %v", err)
		}
		valueLen := uint16(len(item.Value))
		if err := binary.Write(buf, binary.LittleEndian, valueLen); err != nil {
			return fmt.Errorf("failed to write value length: %v", err)
		}
		if _, err := buf.WriteString(item.Value); err != nil {
			return fmt.Errorf("failed to write value: %v", err)
		}
	}
	for _, childOffset := range n.childrenOffsets {
		if err := binary.Write(buf, binary.LittleEndian, childOffset); err != nil {
			return fmt.Errorf("failed to write child offset: %v", err)
		}
	}
	data := buf.Bytes()
	if len(data) > b.pageSize {
		return fmt.Errorf("node data exceeds page size: %d > %d", len(data), b.pageSize)
	}
	padded := make([]byte, b.pageSize)
	copy(padded, data)
	_, err = b.file.WriteAt(padded, offset)
	if err != nil {
		return fmt.Errorf("failed to write node to disk: %v", err)
	}
	n.offset = offset
	return nil
}

// Insert adds a key-value pair to the B-tree.
func (b *Btree) Insert(key string, value interface{}) error {
	if b.threadSafe {
		b.mu.Lock()
		defer b.mu.Unlock()
	}
	valStr, ok := value.(string) // Temporary string restriction
	if !ok {
		return fmt.Errorf("value must be string")
	}
	if b.Length == 0 {
		newNode := &Node{
			items:           []Item{{Key: key, Value: valStr}},
			childrenOffsets: nil,
		}
		offset := b.allocateNode()
		newNode.offset = offset
		if err := b.writeNode(newNode, offset); err != nil {
			return err
		}
		b.RootOffset = offset
		b.Length++
		if err := b.saveHeader(); err != nil {
			return err
		}
		b.cacheNode(newNode)
		return nil
	}
	// Read the root node.
	root, err := b.readNode(b.RootOffset)
	if err != nil {
		return err
	}
	if len(root.items) == 2*b.Degree-1 {
		newRoot := &Node{
			items:           []Item{},
			childrenOffsets: []int64{root.offset},
		}
		newRootOffset := b.allocateNode()
		newRoot.offset = newRootOffset
		if err := b.splitChild(newRoot, 0, root); err != nil {
			return err
		}
		if err := b.insertNonFull(newRoot, key, valStr); err != nil {
			return err
		}
		if err := b.writeNode(newRoot, newRootOffset); err != nil {
			return err
		}
		b.RootOffset = newRootOffset
		b.cacheNode(newRoot) // Cache the new root
	} else {
		if err := b.insertNonFull(root, key, valStr); err != nil {
			return err
		}
	}
	b.Length++
	if err := b.saveHeader(); err != nil { // Save updated metadata
		return err
	}
	return nil
}

// insertNonFull inserts a key-value pair into a node that is guaranteed not to be full.
func (b *Btree) insertNonFull(n *Node, key string, value interface{}) error {
	i := len(n.items) - 1
	if isLeaf(n) {
		// Insert the new item into the correct position.
		n.items = append(n.items, Item{})
		for i >= 0 && key < n.items[i].Key {
			n.items[i+1] = n.items[i]
			i--
		}
		n.items[i+1] = Item{Key: key, Value: value.(string)}
		return b.writeNode(n, n.offset)
	}
	// Find the child which is going to have the new key.
	for i >= 0 && key < n.items[i].Key {
		i--
	}
	i++
	child, err := b.readNode(n.childrenOffsets[i])
	if err != nil {
		return err
	}
	if len(child.items) == 2*b.Degree-1 {
		if err := b.splitChild(n, i, child); err != nil {
			return err
		}
		// Determine which child to descend after split.
		if key > n.items[i].Key {
			i++
		}
		child, err = b.readNode(n.childrenOffsets[i])
		if err != nil {
			return err
		}
	}
	return b.insertNonFull(child, key, value)
}

// splitChild splits the full child node and adjusts the parent accordingly.
func (b *Btree) splitChild(parent *Node, index int, child *Node) error {
	t := b.Degree
	// Median value to move up.
	median := child.items[t-1]
	// Create new node for the second half of child.
	z := &Node{}
	z.items = append([]Item(nil), child.items[t:]...)
	if !isLeaf(child) {
		z.childrenOffsets = append([]int64(nil), child.childrenOffsets[t:]...)
		child.childrenOffsets = child.childrenOffsets[:t]
	}
	child.items = child.items[:t-1]
	zOffset := b.allocateNode()
	z.offset = zOffset
	// Insert z into parent's children.
	if index+1 >= len(parent.childrenOffsets) {
		parent.childrenOffsets = append(parent.childrenOffsets, zOffset)
	} else {
		parent.childrenOffsets = append(parent.childrenOffsets[:index+1],
			append([]int64{zOffset}, parent.childrenOffsets[index+1:]...)...)
	}
	// Insert median into parent's items.
	parent.items = append(parent.items, Item{})
	copy(parent.items[index+1:], parent.items[index:])
	parent.items[index] = median
	// Write updated nodes to disk.
	if err := b.writeNode(child, child.offset); err != nil {
		return err
	}
	if err := b.writeNode(z, z.offset); err != nil {
		return err
	}
	return b.writeNode(parent, parent.offset)
}

// Get retrieves the value associated with the given key, using cache if available.
func (b *Btree) Get(key string) (interface{}, error) {
	if b.threadSafe {
		b.mu.RLock()
		defer b.mu.RUnlock()
	}
	if b.Length == 0 {
		return nil, ports.ErrKeyNotFound
	}
	return b.searchValue(b.RootOffset, key)
}

// searchValue recursively searches for a key starting from the node at the given offset.
func (b *Btree) searchValue(offset int64, key string) (interface{}, error) {
	n, err := b.readNode(offset)
	if err != nil {
		return nil, err
	}
	i := 0
	for i < len(n.items) && key > n.items[i].Key {
		i++
	}
	if i < len(n.items) && key == n.items[i].Key {
		return n.items[i].Value, nil
	}
	if isLeaf(n) {
		return nil, fmt.Errorf("key not found")
	}
	return b.searchValue(n.childrenOffsets[i], key)
}

// Delete removes the key-value pair identified by the key from the B-tree.
func (b *Btree) Delete(key string) error {
	if b.threadSafe {
		b.mu.Lock()
		defer b.mu.Unlock()
	}
	if b.Length == 0 {
		return ports.ErrKeyNotFound
	}
	if err := b.deleteFromNode(b.RootOffset, key); err != nil {
		return err
	}
	// Adjust root if necessary.
	root, err := b.readNode(b.RootOffset)
	if err != nil {
		return err
	}
	if len(root.items) == 0 && !isLeaf(root) {
		b.RootOffset = root.childrenOffsets[0]
		b.cacheNode(root)
	}
	b.Length--
	if err := b.saveHeader(); err != nil { // Save updated metadata
		return err
	}
	return nil
}

// deleteFromNode recursively deletes a key from the subtree rooted at the node with the given offset.
func (b *Btree) deleteFromNode(offset int64, key string) error {
	n, err := b.readNode(offset)
	if err != nil {
		return err
	}
	idx := 0
	for idx < len(n.items) && key > n.items[idx].Key {
		idx++
	}
	if idx < len(n.items) && key == n.items[idx].Key {
		if isLeaf(n) {
			// Case 1: The key is in a leaf node.
			n.items = append(n.items[:idx], n.items[idx+1:]...)
			return b.writeNode(n, offset)
		}
		// Key is in an internal node.
		leftOffset := n.childrenOffsets[idx]
		rightOffset := n.childrenOffsets[idx+1]
		leftChild, err := b.readNode(leftOffset)
		if err != nil {
			return err
		}
		if len(leftChild.items) >= b.Degree {
			pred, err := b.getPredecessor(leftChild)
			if err != nil {
				return err
			}
			n.items[idx] = pred
			if err := b.writeNode(n, n.offset); err != nil {
				return err
			}
			return b.deleteFromNode(leftOffset, pred.Key)
		}
		rightChild, err := b.readNode(rightOffset)
		if err != nil {
			return err
		}
		if len(rightChild.items) >= b.Degree {
			succ, err := b.getSuccessor(rightChild)
			if err != nil {
				return err
			}
			n.items[idx] = succ
			if err := b.writeNode(n, n.offset); err != nil {
				return err
			}
			return b.deleteFromNode(rightOffset, succ.Key)
		}
		// Merge left and right children.
		if err := b.mergeNodes(n, idx); err != nil {
			return err
		}
		return b.deleteFromNode(leftOffset, key)
	}
	// Key is not in this node.
	if isLeaf(n) {
		return fmt.Errorf("key not found")
	}
	childOffset := n.childrenOffsets[idx]
	child, err := b.readNode(childOffset)
	if err != nil {
		return err
	}
	if len(child.items) < b.Degree {
		if err := b.fill(n, idx); err != nil {
			return err
		}
		n, err = b.readNode(n.offset)
		if err != nil {
			return err
		}
		childOffset = n.childrenOffsets[idx]
	}
	return b.deleteFromNode(childOffset, key)
}

// getPredecessor finds the predecessor item (max item in left subtree) for deletion.
func (b *Btree) getPredecessor(n *Node) (Item, error) {
	for !isLeaf(n) {
		lastChildOffset := n.childrenOffsets[len(n.childrenOffsets)-1]
		var err error
		n, err = b.readNode(lastChildOffset)
		if err != nil {
			return Item{}, err
		}
	}
	return n.items[len(n.items)-1], nil
}

// getSuccessor finds the successor item (min item in right subtree) for deletion.
func (b *Btree) getSuccessor(n *Node) (Item, error) {
	for !isLeaf(n) {
		firstChildOffset := n.childrenOffsets[0]
		var err error
		n, err = b.readNode(firstChildOffset)
		if err != nil {
			return Item{}, err
		}
	}
	return n.items[0], nil
}

// mergeNodes merges the child at index idx+1 into the child at index idx of the parent.
func (b *Btree) mergeNodes(parent *Node, idx int) error {
	leftOffset := parent.childrenOffsets[idx]
	rightOffset := parent.childrenOffsets[idx+1]
	left, err := b.readNode(leftOffset)
	if err != nil {
		return err
	}
	right, err := b.readNode(rightOffset)
	if err != nil {
		return err
	}
	left.items = append(left.items, parent.items[idx])
	left.items = append(left.items, right.items...)
	if !isLeaf(left) {
		left.childrenOffsets = append(left.childrenOffsets, right.childrenOffsets...)
	}
	parent.items = append(parent.items[:idx], parent.items[idx+1:]...)
	parent.childrenOffsets = append(parent.childrenOffsets[:idx+1], parent.childrenOffsets[idx+2:]...)
	if err := b.writeNode(left, left.offset); err != nil {
		return err
	}
	return b.writeNode(parent, parent.offset)
}

// fill ensures that the child node at index idx has at least degree items.
func (b *Btree) fill(parent *Node, idx int) error {
	childOffset := parent.childrenOffsets[idx]
	_, err := b.readNode(childOffset)
	if err != nil {
		return err
	}
	if idx > 0 {
		leftSibling, err := b.readNode(parent.childrenOffsets[idx-1])
		if err == nil && len(leftSibling.items) >= b.Degree {
			return b.borrowFromPrev(parent, idx)
		}
	}
	if idx < len(parent.childrenOffsets)-1 {
		rightSibling, err := b.readNode(parent.childrenOffsets[idx+1])
		if err == nil && len(rightSibling.items) >= b.Degree {
			return b.borrowFromNext(parent, idx)
		}
	}
	if idx > 0 {
		return b.mergeNodes(parent, idx-1)
	}
	return b.mergeNodes(parent, idx)
}

// borrowFromPrev borrows an item from the left sibling of the child at index idx.
func (b *Btree) borrowFromPrev(parent *Node, idx int) error {
	childOffset := parent.childrenOffsets[idx]
	child, err := b.readNode(childOffset)
	if err != nil {
		return err
	}
	leftSibling, err := b.readNode(parent.childrenOffsets[idx-1])
	if err != nil {
		return err
	}
	child.items = append([]Item{parent.items[idx-1]}, child.items...)
	if !isLeaf(child) {
		child.childrenOffsets = append([]int64{leftSibling.childrenOffsets[len(leftSibling.childrenOffsets)-1]}, child.childrenOffsets...)
		leftSibling.childrenOffsets = leftSibling.childrenOffsets[:len(leftSibling.childrenOffsets)-1]
	}
	parent.items[idx-1] = leftSibling.items[len(leftSibling.items)-1]
	leftSibling.items = leftSibling.items[:len(leftSibling.items)-1]
	if err := b.writeNode(child, child.offset); err != nil {
		return err
	}
	if err := b.writeNode(leftSibling, leftSibling.offset); err != nil {
		return err
	}
	return b.writeNode(parent, parent.offset)
}

// borrowFromNext borrows an item from the right sibling of the child at index idx.
func (b *Btree) borrowFromNext(parent *Node, idx int) error {
	childOffset := parent.childrenOffsets[idx]
	child, err := b.readNode(childOffset)
	if err != nil {
		return err
	}
	rightSibling, err := b.readNode(parent.childrenOffsets[idx+1])
	if err != nil {
		return err
	}
	child.items = append(child.items, parent.items[idx])
	if !isLeaf(child) {
		child.childrenOffsets = append(child.childrenOffsets, rightSibling.childrenOffsets[0])
		rightSibling.childrenOffsets = rightSibling.childrenOffsets[1:]
	}
	parent.items[idx] = rightSibling.items[0]
	rightSibling.items = rightSibling.items[1:]
	if err := b.writeNode(child, child.offset); err != nil {
		return err
	}
	if err := b.writeNode(rightSibling, rightSibling.offset); err != nil {
		return err
	}
	return b.writeNode(parent, parent.offset)
}

// allocateNode reserves a new page for a node and returns its offset.
func (b *Btree) allocateNode() int64 {
	offset := b.nextOffset
	b.nextOffset += int64(b.pageSize)
	return offset
}

// isLeaf returns true if the node is a leaf node.
func isLeaf(n *Node) bool {
	return len(n.childrenOffsets) == 0
}

func (i Item) Less(than Item) bool {
	return i.Key < than.Key
}

// cacheNode adds or updates a node in the cache with LRU eviction.
// This method is thread-safe and ensures the cache stays within its size limit.
func (b *Btree) cacheNode(n *Node) {
	if b.cacheSize <= 0 {
		return // Caching disabled
	}

	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()

	// If node is already in cache, update it and move to front
	if cached, ok := b.cache[n.offset]; ok {
		if cached.elem != nil { // check for nil elem
			b.cacheList.MoveToFront(cached.elem)
		}
		b.cache[n.offset] = n // Update with latest node data
		return
	}

	// Add new node to cache
	elem := b.cacheList.PushFront(n)
	b.cache[n.offset] = n
	n.elem = elem // Store the list element reference in the node

	// Evict the least recently used node if cache exceeds size limit
	if b.cacheList.Len() > b.cacheSize {
		oldest := b.cacheList.Back()
		if oldest != nil {
			oldNode := oldest.Value.(*Node)
			delete(b.cache, oldNode.offset)
			b.cacheList.Remove(oldest)
			oldNode.elem = nil // Clear reference to avoid memory leak
		}
	}
}

// moveToFront updates the LRU order for an existing cached node.
func (b *Btree) moveToFront(offset int64) {
	b.cacheMu.Lock()
	defer b.cacheMu.Unlock()
	if elem, ok := b.cache[offset]; ok && elem.elem != nil { // Additional nil check
		b.cacheList.MoveToFront(elem.elem)
	}
}

// readNode retrieves a node from cache or disk.
func (b *Btree) readNode(offset int64) (*Node, error) {
	// Check cache first
	if b.cacheSize > 0 {
		b.cacheMu.RLock()
		if node, ok := b.cache[offset]; ok {
			b.cacheMu.RUnlock()
			b.moveToFront(offset) // Update LRU
			return node, nil
		}
		b.cacheMu.RUnlock()
	}

	// Read from disk if not cached
	node, err := b.readNodeFromDisk(offset)
	if err != nil {
		return nil, err
	}

	// Cache the node
	if b.cacheSize > 0 {
		b.cacheNode(node)
	}
	return node, nil
}

// writeNode writes a node to disk and updates the cache.
func (b *Btree) writeNode(n *Node, offset int64) error {
	err := b.writeNodeToDisk(n, offset)
	if err != nil {
		return err
	}
	if b.cacheSize > 0 {
		b.cacheNode(n) // Update cache after write
	}
	return nil
}
