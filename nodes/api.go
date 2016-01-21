package nodes

func Get() ([]string, error) {
	return get()
}

func GetByCluster(cluster string) ([]string, error) {
	return getByCluster(cluster)
}
