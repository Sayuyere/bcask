package index

import "fmt"

type Index interface {
	// Get retrieves the value associated with the given key.
	Get(key string) (*IndexValue, error)
	// Set associates the given value with the specified key.
	Set(key string, value *IndexValue) error
	// Delete removes the key-value pair associated with the given key.
	Delete(key string) error
	// Exists checks if the key exists in the index.
	Exists(key string) (bool, error)
	// Close releases any resources held by the index.
	Close() error
	// Iterate returns a channel to iterate over all key-value pairs in the index.
	Iterate() (<-chan map[string]*IndexValue, error)
	// Count returns the number of key-value pairs in the index.
	Count() (int, error)
	// Clear removes all key-value pairs from the index.
	Clear() error
	// Encode serializes the index to a byte slice.
	Encode() ([]byte, error)
	// Decode deserializes the index from a byte slice.
	Decode(data []byte) error
}

type IndexValue struct {
	FileID    string `json:"file_id"`
	ValueSize int64  `json:"value_size"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

type PrefixTrieNode struct {
	Children map[rune]*PrefixTrieNode `json:"children"`
	IsEnd    bool                     `json:"is_end"`
	Value    *IndexValue              `json:"value"`
}

type PrefixTrie struct {
	Root *PrefixTrieNode `json:"root"`
}

func (t *PrefixTrie) Close() error {
	// No resources to release in this implementation
	return nil
}

func (t *PrefixTrie) Get(key string) (*IndexValue, error) {
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

func (t *PrefixTrie) Set(key string, value *IndexValue) error {
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

func (t *PrefixTrie) Iterate() (<-chan map[string]*IndexValue, error) {
	ch := make(chan map[string]*IndexValue)
	go func() {
		var iterateNodes func(node *PrefixTrieNode, prefix string)
		iterateNodes = func(node *PrefixTrieNode, prefix string) {
			if node.IsEnd {
				ch <- map[string]*IndexValue{prefix: node.Value}
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
	// Implement serialization logic here
	return nil, nil // Placeholder
}

func (t *PrefixTrie) Decode(data []byte) error {
	// Implement deserialization logic here
	return nil // Placeholder
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
