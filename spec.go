package index

type Service interface {
	Boot()
	Create(namespace, namespaceA, namespaceB, valueA, valueB string) error
	Delete(namespace, namespaceA, namespaceB, valueA string) error
	Exists(namespace, namespaceA, namespaceB, valueA string) (bool, error)
	Search(namespace, namespaceA, namespaceB, valueA string) (string, error)
	Shutdown()
}
