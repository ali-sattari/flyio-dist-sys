package kafka

import (
	"fmt"
	"hash/crc32"
)

func getNodeID(s string) int64 {
	hash := crc32.ChecksumIEEE([]byte(s))
	return int64(hash & ((1 << 10) - 1)) // Constrain to 10 bits
}

func formatCommittedOffsetKey(key string) string {
	return fmt.Sprintf("co_%s", key)
}

func formatOffsetListKey(key string) string {
	return fmt.Sprintf("ol_%s", key)
}

func formatMsgKey(key string, offset int64) string {
	return fmt.Sprintf("%s:%d", key, offset)
}
