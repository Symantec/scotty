package cis

import (
	"fmt"
	"strings"
)

const (
	kCisPath = "packages"
)

var (
	replacer = strings.NewReplacer(".", "_")
)

func marshal(stats *Stats) interface{} {

	type versionSizeType struct {
		Version string `json:"version"`
		Size    uint64 `json:"size"`
	}

	type packagesType struct {
		PkgMgmtType string                     `json:"pkgMgmtType"`
		Packages    map[string]versionSizeType `json:"installed"`
	}

	jsonToEncode := &packagesType{
		PkgMgmtType: stats.Packages.ManagementType,
		Packages: make(
			map[string]versionSizeType,
			len(stats.Packages.Packages)),
	}

	for _, entry := range stats.Packages.Packages {
		jsonToEncode.Packages[replacer.Replace(entry.Name)] = versionSizeType{
			Version: entry.Version, Size: entry.Size}
	}
	return jsonToEncode
}

func (c *Client) write(stats *Stats) error {
	jsonToEncode := marshal(stats)
	url := fmt.Sprintf("%s/%s/%s/%s", c.endpoint, kCisPath, c.dataCenter, stats.InstanceId)
	if c.async != nil {
		c.async.Send(url, jsonToEncode)
		return nil
	}
	err := c.sync.Write(url, nil, jsonToEncode)
	if err != nil {
		return fmt.Errorf("%s: %v", url, err)
	}
	return nil
}

func (c *Client) writeAll(stats []Stats) error {
	jsonToEncode := make(map[string]interface{}, len(stats))
	for _, stat := range stats {
		jsonToEncode[stat.InstanceId] = marshal(&stat)
	}
	url := fmt.Sprintf("%s/%s/%s", c.endpoint, kCisPath, c.dataCenter)
	if c.async != nil {
		c.async.Send(url, jsonToEncode)
		return nil
	}
	return c.sync.Write(url, nil, jsonToEncode)
}
