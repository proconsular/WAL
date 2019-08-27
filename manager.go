package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

// SegmentManager manages log segments
type SegmentManager struct {
	segments  []Segment
	threshold int
	active    int
}

func makeManager(threshold int) *SegmentManager {
	return &SegmentManager{make([]Segment, 0), threshold, 0}
}

func (sm *SegmentManager) init() {
	os.Mkdir("data", 0777)
	folder, _ := os.Open("data")
	defer folder.Close()
	files, _ := folder.Readdirnames(0)
	for _, file := range files {
		if strings.HasSuffix(file, ".log") {
			id, count := getSegmentInfo(file)
			sm.segments = append(sm.segments, Segment{id, count})
		}
	}
	sort.Sort(ByID(sm.segments))
	sm.active = len(sm.segments) - 1
	if len(sm.segments) == 0 {
		sm.active = 0
		sm.createNewSegment()
	}
}

func getSegmentInfo(name string) (int, int) {
	file, err := os.Open("data/" + name)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	totalData := make([]byte, 0)
	for {
		data := make([]byte, 512)
		size, err := file.Read(data)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		totalData = append(totalData, data[:size]...)
		if size < 512 {
			break
		}
	}
	text := string(totalData)
	id, _ := strconv.Atoi(strings.Split(strings.Split(name, "-")[1], ".")[0])
	return id, strings.Count(text, "\n")
}

func (sm *SegmentManager) createNewSegment() {
	id := 0
	if len(sm.segments) > 0 {
		sm.active++
		id = sm.segments[sm.active-1].id + 1
	}
	os.Create("data/segment-" + strconv.Itoa(id) + ".log")
	sm.segments = append(sm.segments, Segment{id, 0})
}

func (sm *SegmentManager) run() {
	for {
		if len(sm.segments) > 2 {
			sm.compact()
		}
		fmt.Println("Compaction Check.")
		time.Sleep(60 * time.Second)
	}
}

func (sm *SegmentManager) compact() {
	table := make(map[string]string)
	for index := range sm.segments[:sm.active] {
		file, err := os.Open("data/" + sm.getSegmentName(index))
		if err != nil {
			panic(err)
		}
		defer file.Close()
		for record := readOneRecord(file); record != ""; record = readOneRecord(file) {
			key, value := parseRecord(record)
			table[key] = value
		}
	}
	file, err := os.Create("data/compact.log")
	if err != nil {
		panic(err)
	}
	for key, value := range table {
		file.Write([]byte(key + ":" + value + "\n"))
	}
	file.Sync()
	file.Close()
	for index := range sm.segments[:sm.active] {
		os.Remove("data/" + sm.getSegmentName(index))
	}
	os.Rename("data/compact.log", "data/"+sm.getSegmentName(0))
	for i := sm.active - 1; i >= 1; i-- {
		sm.segments = append(sm.segments[:i], sm.segments[i+1:]...)
	}
	sort.Sort(ByID(sm.segments))
	first := sm.segments[0]
	first.length = len(table)
	sm.segments[0] = first
	sm.active = len(sm.segments) - 1
}

func (sm *SegmentManager) getCurrentSegmentName() string {
	return "segment-" + strconv.Itoa(sm.segments[sm.active].id) + ".log"
}

func (sm *SegmentManager) getSegmentName(index int) string {
	return "segment-" + strconv.Itoa(sm.segments[index].id) + ".log"
}

func (sm *SegmentManager) write(key, value string) {
	log, _ := os.OpenFile("data/"+sm.getCurrentSegmentName(), os.O_WRONLY, 0777)
	log.Seek(0, 2)
	log.Write([]byte(key + ":" + value + "\n"))
	log.Close()
	segment := sm.segments[sm.active]
	segment.length++
	sm.segments[sm.active] = segment
	if segment.length >= sm.threshold {
		sm.createNewSegment()
	}
}

func (sm *SegmentManager) read(key string) string {
	latest := ""
	for index := range sm.segments {
		value := sm.readSegment(index, key)
		if value != "" {
			latest = value
		}
	}
	return latest
}

func (sm *SegmentManager) readSegment(index int, key string) string {
	log, _ := os.Open("data/" + sm.getSegmentName(index))
	log.Seek(0, 0)
	latest := ""
	for {
		record := readOneRecord(log)
		if record == "" {
			break
		}
		rKey, rValue := parseRecord(record)
		if key == rKey {
			latest = rValue
		}
	}
	return latest
}

func parseRecord(record string) (string, string) {
	index := 0
	for string(record[index]) != ":" {
		index++
	}
	return record[:index], record[index+1:]
}

func readOneRecord(log *os.File) string {
	record := ""
	for {
		buffer := make([]byte, 1)
		_, err := log.Read(buffer)
		if err != nil {
			return ""
		}
		if string(buffer) != "\n" {
			record = record + string(buffer)
		} else {
			return record
		}
	}
}
