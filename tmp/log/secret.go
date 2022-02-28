package log

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

func Secret(secret string) string {
	var mask bytes.Buffer
	if len(secret) > 16 {
		mask.WriteString(secret[:4])
		mask.WriteString("****")
		mask.WriteString(secret[len(secret)-4:])
	} else {
		mask.WriteString("****")
	}
	mask.WriteString(fmt.Sprintf("(CRC-32c: %08X)", crc32.Checksum([]byte(secret), crc32.IEEETable)))
	return mask.String()
}
