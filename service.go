// Package index implements Service to map indizes.
package index

import (
	"fmt"
	"sync"

	"github.com/the-anna-project/storage"
	storageerror "github.com/the-anna-project/storage/error"
)

// ConfigService represents the configuration used to create a new index
// service.
type ServiceConfig struct {
	// Dependencies.
	StorageCollection *storage.Collection
}

// DefaultConfig provides a default configuration to create a new index service
// by best effort.
func DefaultServiceConfig() ServiceConfig {
	var err error

	var storageCollection *storage.Collection
	{
		storageConfig := storage.DefaultCollectionConfig()
		storageCollection, err = storage.NewCollection(storageConfig)
		if err != nil {
			panic(err)
		}
	}

	config := ServiceConfig{
		// Dependencies.
		StorageCollection: storageCollection,
	}

	return config
}

// NewService creates a new index service.
func NewService(config ServiceConfig) (Service, error) {
	// Dependencies.
	if config.StorageCollection == nil {
		return nil, maskAnyf(invalidConfigError, "storage collection must not be empty")
	}

	newService := &service{
		// Dependencies.
		storage: config.StorageCollection,

		// Internals.
		bootOnce:     sync.Once{},
		closer:       make(chan struct{}, 1),
		shutdownOnce: sync.Once{},
	}

	return newService, nil
}

type service struct {
	// Dependencies.
	storage *storage.Collection

	// Internals.
	bootOnce     sync.Once
	closer       chan struct{}
	shutdownOnce sync.Once
}

func (s *service) Boot() {
	s.bootOnce.Do(func() {
		// Service specific boot logic goes here.
	})
}

func (s *service) Create(namespace, namespaceA, namespaceB, valueA, valueB string) error {
	key := s.key(namespace, namespaceA, namespaceB, valueA)

	// We only want to create a new index mapping in case there does none exist
	// yet. For updating purposes clients have to use Service.Update.
	ok, err := s.storage.Index.Exists(key)
	if err != nil {
		return maskAny(err)
	}
	if ok {
		return nil
	}

	err = s.storage.Index.Set(key, valueB)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Delete(namespace, namespaceA, namespaceB, valueA string) error {
	key := s.key(namespace, namespaceA, namespaceB, valueA)

	err := s.storage.Index.Remove(key)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) Exists(namespace, namespaceA, namespaceB, valueA string) (bool, error) {
	_, err := s.Search(namespace, namespaceA, namespaceB, valueA)
	if IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, maskAny(err)
	}

	return true, nil
}

func (s *service) Search(namespace, namespaceA, namespaceB, valueA string) (string, error) {
	key := s.key(namespace, namespaceA, namespaceB, valueA)

	result, err := s.storage.Index.Get(key)
	if storageerror.IsNotFound(err) {
		return "", maskAnyf(notFoundError, key)
	} else if err != nil {
		return "", maskAny(err)
	}

	return result, nil
}

func (s *service) Shutdown() {
	s.shutdownOnce.Do(func() {
		close(s.closer)
	})
}

func (s *service) Update(namespace, namespaceA, namespaceB, valueA, valueB string) error {
	key := s.key(namespace, namespaceA, namespaceB, valueA)

	// We only want to update an index mapping in case there does one exist.
	ok, err := s.storage.Index.Exists(key)
	if err != nil {
		return maskAny(err)
	}
	if !ok {
		return maskAnyf(notFoundError, key)
	}

	err = s.storage.Index.Set(key, valueB)
	if err != nil {
		return maskAny(err)
	}

	return nil
}

func (s *service) key(namespace, namespaceA, namespaceB, valueA string) string {
	return fmt.Sprintf("%s:%s:%s:%s", namespace, namespaceA, namespaceB, valueA)
}
