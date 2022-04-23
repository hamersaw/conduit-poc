package conduit

import (
	"sync"
)

var exists = struct{}{}

type Set struct{
	sync.Map
}

func (s *Set) Add(value string) {
	s.Store(value, exists)
}

func (s *Set) Contains(value string) bool {
    _, ok := s.Load(value)
    return ok
}
