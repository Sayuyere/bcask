package index

import (
	"fmt"
	"sync"

	"github.com/sayuyere/bcask/internal/item"
	"github.com/vmihailenco/msgpack/v5"
)

type Index interface {
	// Get retrieves the value associated with the given key.
	Get(key string) (*item.MemoryItem, error)
	// Set associates the given value with the specified key.
	Set(key string, value *item.MemoryItem) error
	// Delete removes the key-value pair associated with the given key.
	Delete(key string) error
	// Exists checks if the key exists in the index.
	Exists(key string) (bool, error)
	// Close releases any resources held by the index.
	Close() error
	// Iterate returns a channel to iterate over all key-value pairs in the index.
	Iterate() (<-chan map[string]*item.MemoryItem, error)
	// Count returns the number of key-value pairs in the index.
	Count() (int, error)
	// Clear removes all key-value pairs from the index.
	Clear() error
	// Encode serializes the index to a byte slice.
	Encode() ([]byte, error)
	// Decode deserializes the index from a byte slice.
	Decode(data []byte) error
}

type PrefixTrieNode struct {
	Children map[rune]*PrefixTrieNode `json:"children"`
	IsEnd    bool                     `json:"is_end"`
	Value    *item.MemoryItem         `json:"value"`
	RWLock   sync.RWMutex             `json:"-"`
}

type PrefixTrie struct {
	Root *PrefixTrieNode `json:"root"`
}

func (t *PrefixTrie) Close() error {
	// No resources to release in this implementation
	return nil
}
func (t *PrefixTrie) Get(key string) (*item.MemoryItem, error) {
	t.Root.RWLock.RLock()
	defer t.Root.RWLock.RUnlock()
	if t.Root == nil {
		return nil, fmt.Errorf("trie is not initialized")
	}
	node := t.Root
	for _, char := range key {
		if _, exists := node.Children[char]; !exists {
			return nil, fmt.Errorf("key not found")
		}
		node = node.Children[char]
	}
	if node.IsEnd {
		return node.Value, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (t *PrefixTrie) Set(key string, value *item.MemoryItem) error {
	t.Root.RWLock.Lock()
	defer t.Root.RWLock.Unlock()
	node := t.Root
	for _, char := range key {
		if _, exists := node.Children[char]; !exists {
			node.Children[char] = &PrefixTrieNode{
				Children: make(map[rune]*PrefixTrieNode),
			}
		}
		node = node.Children[char]
	}
	node.IsEnd = true
	node.Value = value
	return nil
}

func (t *PrefixTrie) Delete(key string) error {
	t.Root.RWLock.Lock()
	defer t.Root.RWLock.Unlock()
	node := t.Root
	stack := []*PrefixTrieNode{node}
	for _, char := range key {
		if _, exists := node.Children[char]; !exists {
			return nil // Key does not exist
		}
		node = node.Children[char]
		stack = append(stack, node)
	}
	if !node.IsEnd {
		return nil // Key does not exist
	}
	node.IsEnd = false
	node.Value = nil

	// Clean up empty nodes
	for i := len(stack) - 1; i > 0; i-- {
		parent := stack[i-1]
		char := rune(key[i-1])
		if len(parent.Children[char].Children) == 0 && !parent.Children[char].IsEnd {
			delete(parent.Children, char)
		} else {
			break
		}
	}
	return nil
}

func (t *PrefixTrie) Exists(key string) (bool, error) {
	t.Root.RWLock.RLock()
	defer t.Root.RWLock.RUnlock()
	node := t.Root
	for _, char := range key {
		if _, exists := node.Children[char]; !exists {
			return false, nil
		}
		node = node.Children[char]
	}
	return node.IsEnd, nil
}

func (t *PrefixTrie) Count() (int, error) {
	t.Root.RWLock.RLock()
	defer t.Root.RWLock.RUnlock()
	count := 0
	var countNodes func(node *PrefixTrieNode)
	countNodes = func(node *PrefixTrieNode) {
		if node.IsEnd {
			count++
		}
		for _, child := range node.Children {
			countNodes(child)
		}
	}
	countNodes(t.Root)
	return count, nil
}

func (t *PrefixTrie) Iterate() (<-chan map[string]*item.MemoryItem, error) {
	// Ensure that channel is getting consumed properly else Trie updates will block
	t.Root.RWLock.RLock()
	defer t.Root.RWLock.RUnlock()
	ch := make(chan map[string]*item.MemoryItem)
	go func() {
		var iterateNodes func(node *PrefixTrieNode, prefix string)
		iterateNodes = func(node *PrefixTrieNode, prefix string) {
			if node.IsEnd {
				ch <- map[string]*item.MemoryItem{prefix: node.Value}
			}
			for char, child := range node.Children {
				iterateNodes(child, prefix+string(char))
			}
		}
		iterateNodes(t.Root, "")
		close(ch)
	}()
	return ch, nil
}

func (t *PrefixTrie) Encode() ([]byte, error) {
	// Serialize using MessagePack
	t.Root.RWLock.RLock()
	defer t.Root.RWLock.RUnlock()
	data, err := msgpack.Marshal(t.Root)
	if err != nil {
		return nil, fmt.Errorf("failed to encode trie: %v", err)
	}
	return data, nil
}

func (t *PrefixTrie) Decode(data []byte) error {
	// Implement deserialization logic here
	t.Root.RWLock.Lock()
	defer t.Root.RWLock.Unlock()
	var root PrefixTrieNode
	if err := msgpack.Unmarshal(data, &root); err != nil {
		return fmt.Errorf("failed to decode trie: %v", err)
	}
	t.Root = &root
	return nil
}

func (t *PrefixTrie) Clear() error {
	t.Root = &PrefixTrieNode{
		Children: make(map[rune]*PrefixTrieNode),
	}
	return nil
}
func NewPrefixTrie() *PrefixTrie {
	return &PrefixTrie{
		Root: &PrefixTrieNode{
			Children: make(map[rune]*PrefixTrieNode),
		},
	}
}
