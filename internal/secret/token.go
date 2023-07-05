package secret

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

func Token(token string) string {
	var mask bytes.Buffer
	if len(token) > 16 {
		mask.WriteString(token[:4])
		mask.WriteString("****")
		mask.WriteString(token[len(token)-4:])
	} else {
		mask.WriteString("****")
	}
	mask.WriteString(fmt.Sprintf("(CRC-32c: %08X)", crc32.Checksum([]byte(token), crc32.IEEETable)))
	return mask.String()
}
