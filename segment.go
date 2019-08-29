package main

// Segment tracking info
type Segment struct {
	id     int
	length int
}

// ByID sort by id
type ByID []Segment

func (a ByID) Len() int           { return len(a) }
func (a ByID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByID) Less(i, j int) bool { return a[i].id < a[j].id }
