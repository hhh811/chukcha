package server

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCutToLastMessage(t *testing.T) {
	res := []byte("100\n101\n10")

	wantTruncated, wantRest := []byte("100\n101\n"), []byte("10")
	gotTruncated, gotRest, err := cutToLastMessage(res)

	require.NoError(t, err)
	require.Equal(t, wantTruncated, gotTruncated)
	require.Equal(t, wantRest, gotRest)
}

func TestCutToLastMessageErrors(t *testing.T) {
	res := []byte("100000")

	_, _, err := cutToLastMessage(res)

	require.NotEmpty(t, err)
}
