package secret

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

const minTokenLength = 16

// Token returns a redacted representation of token safe for logs and error messages.
// Tokens longer than 16 bytes keep the first and last four characters with the middle
// replaced by "****"; shorter tokens are fully replaced by "****".
// A CRC-32c checksum of the original value is appended so identical tokens can be
// correlated in logs without exposing the secret.
func Token(token string) string {
	var mask bytes.Buffer
	if len(token) > minTokenLength {
		mask.WriteString(token[:4])
		mask.WriteString("****")
		mask.WriteString(token[len(token)-4:])
	} else {
		mask.WriteString("****")
	}
	fmt.Fprintf(&mask, "(CRC-32c: %08X)", crc32.Checksum([]byte(token), crc32.IEEETable))

	return mask.String()
}
