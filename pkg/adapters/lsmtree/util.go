package lsmtree

import (
	"hash/crc32"
)

// ComputeChecksum calculates the CRC32 checksum of the given data.
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
