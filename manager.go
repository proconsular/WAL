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

// SegmentManager manages log segments
type SegmentManager struct {
	segments  []Segment
	threshold int
	active    int
	cache     *Cache
}

func makeManager(threshold int) *SegmentManager {
	return &SegmentManager{make([]Segment, 0), threshold, 0, MakeCache(10)}
}

func (sm *SegmentManager) init() {
	os.Mkdir("data", 0777)
	folder, _ := os.Open("data")
	defer folder.Close()
	file, err := os.Open("data/heap.bin")
	if os.IsNotExist(err) {
		os.Create("data/heap.bin")
	}
	file.Close()
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
	info, _ := file.Stat()
	id, _ := strconv.Atoi(strings.Split(strings.Split(name, "-")[1], ".")[0])
	return id, int(info.Size())
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
	for index := range sm.segments {
		file, err := os.Open("data/" + sm.getSegmentName(index))
		if err != nil {
			panic(err)
		}
		defer file.Close()
		for record := readOneRecord(file); record != ""; record = readOneRecord(file) {
			key, value := parseRecord(record)
			table[key] = sm.translateOffset(decodeInt64([]byte(value)))
		}
	}
	heap, _ := os.Create("data/temp-heap.bin")
	offsetTable := make(map[string]int64)
	offset := int64(0)
	for key, value := range table {
		data := append(encodeInt64(int64(len(value))), []byte(value)...)
		heap.Write(data)
		offsetTable[key] = offset
		offset += int64(len(data))
	}
	heap.Sync()
	heap.Close()
	file, _ := os.Create("data/compact.log")
	size := 0
	for key, value := range offsetTable {
		data := []byte(key + ":" + string(encodeInt64(value)) + "\n")
		size += len(data)
		file.Write(data)
	}
	file.Sync()
	file.Close()
	os.Remove("data/heap.bin")
	os.Rename("data/temp-heap.bin", "data/heap.bin")
	for index := range sm.segments {
		os.Remove("data/" + sm.getSegmentName(index))
	}
	os.Rename("data/compact.log", "data/"+sm.getSegmentName(0))
	for i := sm.active - 1; i >= 1; i-- {
		sm.segments = append(sm.segments[:i], sm.segments[i+1:]...)
	}
	sort.Sort(ByID(sm.segments))
	first := sm.segments[0]
	first.length = size
	sm.segments[0] = first
	sm.active = len(sm.segments) - 1
}

func copy(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func (sm *SegmentManager) getCurrentSegmentName() string {
	return "segment-" + strconv.Itoa(sm.segments[sm.active].id) + ".log"
}

func (sm *SegmentManager) getSegmentName(index int) string {
	return "segment-" + strconv.Itoa(sm.segments[index].id) + ".log"
}

func (sm *SegmentManager) write(key, value string) {
	heap, _ := os.OpenFile("data/heap.bin", os.O_WRONLY, 0777)
	info, _ := heap.Stat()
	offset := info.Size()
	heap.Seek(0, 2)
	heap.Write(append(encodeInt64(int64(len(value))), []byte(value)...))
	heap.Close()

	log, _ := os.OpenFile("data/"+sm.getCurrentSegmentName(), os.O_WRONLY, 0777)
	log.Seek(0, 2)
	data := []byte(key + ":" + string(encodeInt64(offset)) + "\n")
	log.Write(data)
	log.Close()
	segment := sm.segments[sm.active]
	segment.length += len(data)
	sm.segments[sm.active] = segment
	if segment.length >= sm.threshold {
		sm.createNewSegment()
	}
	sm.cache.Set(key, value)
}

func (sm *SegmentManager) read(key string) string {
	val := sm.cache.Get(key)
	if val != "" {
		return val
	}
	latest := ""
	for index := range sm.segments {
		value := sm.readSegment(index, key)
		if value != "" {
			latest = value
		}
	}
	if latest != "" {
		offset := decodeInt64([]byte(latest))
		record := sm.translateOffset(offset)
		sm.cache.Set(key, record)
		return record
	}
	return latest
}

func (sm *SegmentManager) translateOffset(offset int64) string {
	heap, _ := os.Open("data/heap.bin")
	heap.Seek(offset, 0)
	sizeBuffer := make([]byte, 8)
	heap.Read(sizeBuffer)
	size := decodeInt64(sizeBuffer)
	dataBuffer := make([]byte, size)
	heap.Read(dataBuffer)
	defer heap.Close()
	return string(dataBuffer)
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
		if string(buffer) != ":" {
			record = record + string(buffer)
		} else {
			if len(record) > 0 {
				buffer = make([]byte, 8)
				log.Read(buffer)
				record += ":" + string(buffer)
				log.Seek(1, 1)
			}
			return record
		}
	}
}

func encodeInt64(value int64) []byte {
	data := make([]byte, 8)
	for i := 0; i < 8; i++ {
		data[i] |= byte((uint64(value) >> (uint64(i) * 8)))
	}
	return data
}

func decodeInt64(data []byte) int64 {
	value := int64(0)
	for i := 0; i < 8; i++ {
		value |= int64(data[i] << byte(i*8))
	}
	return value
}
