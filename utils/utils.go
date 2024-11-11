package utils

import (
	"fmt"
	"strings"
)

func NormalizeLocation(lac int, cellID int) (string, error) {
	sb := &strings.Builder{}
	var err error
	if lac == 0 {
		_, err = fmt.Fprintf(sb, "%d", cellID)
	} else {
		_, err = fmt.Fprintf(sb, "%d-%d", lac, cellID)
	}
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}
