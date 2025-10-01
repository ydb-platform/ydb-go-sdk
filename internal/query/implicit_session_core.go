package query

import "context"

type implicitSessionCore struct {
	sessionCore
}

func (s *implicitSessionCore) Close(context.Context) error {
	return nil
}
