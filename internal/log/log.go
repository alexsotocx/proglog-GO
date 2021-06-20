package log

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/alexsotocx/proglog/api/v1"
)

type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (log *Log) setup() error {
	files, err := ioutil.ReadDir(log.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })
	for i := 0; i < len(baseOffsets); i++ {
		if err = log.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		i++
	}
	if log.segments == nil {
		if err = log.newSegment(log.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Append(record *api.Record) (uint64, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	off, err := log.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if log.activeSegment.IsMaxed() {
		err = log.newSegment(off + 1)
	}
	return off, err
}

func (log *Log) Read(off uint64) (*api.Record, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	var seg *segment
	for _, segment := range log.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			seg = segment
			break
		}
	}
	if seg == nil || seg.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return seg.Read(off)
}

func (log *Log) Close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	for _, segment := range log.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (log *Log) Remove() error {
	if err := log.Close(); err != nil {
		return err
	}
	return os.RemoveAll(log.Dir)
}

func (log *Log) Reset() error {
	if err := log.Remove(); err != nil {
		return err
	}
	return log.setup()
}

func (log *Log) LowestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.segments[0].baseOffset, nil
}

func (log *Log) HighestOffset() (uint64, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	off := log.segments[len(log.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (log *Log) Truncate(lowest uint64) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	var segments []*segment
	for _, s := range log.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)

	}
	log.segments = segments
	return nil
}

func (log *Log) Reader() io.Reader {
	log.mu.RLock()
	defer log.mu.RUnlock()
	readers := make([]io.Reader, len(log.segments))
	for i, segment := range log.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (log *Log) newSegment(off uint64) error {
	s, err := newSegment(log.Dir, off, log.Config)
	if err != nil {
		return err
	}
	log.segments = append(log.segments, s)
	log.activeSegment = s
	return nil
}
