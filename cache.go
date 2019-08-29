package main

// Cache stores key-value pairs
type Cache struct {
	table    map[string]string
	priority []string
	size     int
}

// MakeCache makes cache
func MakeCache(size int) *Cache {
	return &Cache{make(map[string]string), make([]string, size), size}
}

// Get gets value
func (c *Cache) Get(key string) string {
	val, ok := c.table[key]
	if ok {
		return val
	}
	return ""
}

// Set sets value
func (c *Cache) Set(key, value string) {
	c.table[key] = value
}
