package credentials

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_parseExpiresAt(t *testing.T) {
	//nolint:lll
	expiresAt, err := parseExpiresAt("eyJhbGciOiJQUzI1NiIsImtpZCI6IjQzIn0.eyJhdWQiOiJcL2RldjAyIiwiZXhwIjoxNjYwNjk1MzIyLCJpYXQiOjE2NjA2NTIxMjIsInN1YiI6InJvb3QifQ.qLAyzd7Fa9sDt1bZ78m-pmMSF8aKtPH8sT3hPEUaB4k5vXX3mZiZktsj9KD523Njs6O57TtbLKxfQIdJTdB6BGudNmmbAvlvBJOU6_WCJvQI3UpntFY1Yj-KPO8pbGgX6-UhTMcXmCbPzeEZd7RE1r9D79vXJqOHabdWAgIVpSGMMvCWS68Edw-r8EPALjgwHZQGiPz6bHdF4mg1jswLGEwJ_BPflk4kSp7I8MIj_h4OgUvu5JSmrnQ5vjmGklx4iUxVllkdCVZ2MRALzYe5xR0dw_m5tUdeiJpzQvvuB4zyNZKveWnwUevJWA7VUABRbNiBFiSFXISWLWpMOruyvg")
	require.NoError(t, err)
	require.Equal(t, time.Unix(1660695322, 0), expiresAt)
}
