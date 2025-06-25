package consts

import "errors"

const SegmentPrefix string = "segment_file_"
const SegmentMaxSize int64 = 1024 * 1024 * 4 //4MB Segment Size
const IndexFileName string = "index_file"

var ErrorSegmentCapacityFull error = errors.New("segment capacity full: reached maximum segment size, need to create a new segment")
var ErrorMMapIncompleteWrite error = errors.New("incomplete write: not all data could be written to the memory-mapped segment")
var ErrorInvalidOffset error = errors.New("invalid offset: offset is out of bounds for the segment")
var ErrorDiskKeyValueBigEntry error = errors.New("disk key value size is greater than the segment size so we can't store it")
