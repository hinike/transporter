package commitlog_test

import (
	"io"
	"testing"

	"github.com/compose/transporter/commitlog"
)

var (
	readTests = []struct {
		name, path string
		offset     int64
	}{
		{
			"one_segment",
			"testdata/reader_one_segment",
			-1,
		},
		{
			"many_segments",
			"testdata/reader_many_segments",
			-1,
		},
		{
			"many_segments_offset_equal_base_offset",
			"testdata/reader_many_segments",
			0,
		},
		{
			"many_segments_offset_in_first_segment",
			"testdata/reader_many_segments",
			28339,
		},
		{
			"many_segments_offset_equal_base_offset",
			"testdata/reader_many_segments",
			28340,
		},
		{
			"many_segments_offset_in_middle_segment",
			"testdata/reader_many_segments",
			56679,
		},
		{
			"many_segments_offset_equal_base_offset",
			"testdata/reader_many_segments",
			56680,
		},
		{
			"many_segments_offset_in_last_segment",
			"testdata/reader_many_segments",
			85015,
		},
	}
)

func TestRead(t *testing.T) {
	for _, rt := range readTests {
		c, err := commitlog.New(
			commitlog.WithPath(rt.path),
		)
		if err != nil {
			t.Fatalf("[%s] unexpected commitlog.New error, %s", rt.name, err)
		}
		r, err := c.NewReader(rt.offset)
		if err != nil {
			t.Fatalf("[%s] unexpected NewReader error, %s", rt.name, err)
		}
		for {
			header := make([]byte, commitlog.LogEntryHeaderLen)
			if _, err := r.Read(header); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("[%s] unexpected Read error, %s", rt.name, err)
			}
			size := commitlog.Encoding.Uint32(header[8:12])
			kvBytes := make([]byte, size)
			if _, err := r.Read(kvBytes); err != nil {
				if err != io.EOF {
					break
				}
				t.Fatalf("[%s] unexpected Read error, %s", rt.name, err)
			}
		}
	}
}
