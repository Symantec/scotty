package cloudhealth

func (b *Buffer) add(instance InstanceData, fss []FsData) bool {
	if b.dataPoints()+InstanceDataPointCount+len(fss)*FsDataPointCount > MaxDataPoints {
		return false
	}
	b.instances = append(b.instances, instance)
	b.fss = append(b.fss, fss...)
	return true
}

func (b *Buffer) get() (instances []InstanceData, fss []FsData) {
	instances = make([]InstanceData, len(b.instances))
	fss = make([]FsData, len(b.fss))
	copy(instances, b.instances)
	copy(fss, b.fss)
	return
}

func (b *Buffer) dataPoints() int {
	return len(b.instances)*InstanceDataPointCount + len(b.fss)*FsDataPointCount
}
