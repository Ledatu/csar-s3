package intent

import (
	"sync"

	"github.com/ledatu/csar-core/storage"
)

type Store struct {
	mu      sync.RWMutex
	intents map[string]storage.UploadIntent
}

func NewStore() *Store {
	return &Store{
		intents: make(map[string]storage.UploadIntent),
	}
}

func (s *Store) Put(intent storage.UploadIntent) storage.UploadIntent {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intents[intent.ID] = intent
	return intent
}

func (s *Store) Get(id string) (storage.UploadIntent, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intent, ok := s.intents[id]
	return intent, ok
}
