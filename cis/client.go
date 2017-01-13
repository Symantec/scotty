package cis

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

var (
	replacer = strings.NewReplacer(".", "_")
)

func (c *Client) write(stats *Stats) error {

	type versionSizeType struct {
		Version string `json:"version"`
		Size    uint64 `json:"size"`
	}

	type packagesType struct {
		PkgMgmtType string                     `json:"pkgMgmtType"`
		Packages    map[string]versionSizeType `json:"packages"`
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

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	if err := encoder.Encode(jsonToEncode); err != nil {
		return err
	}
	// TODO: Maybe get rid of all the diagnostics in the error messages
	bufferStr := buffer.String()
	url := fmt.Sprintf("%s/aws/instances/%s", c.endpoint, stats.InstanceId)
	resp, err := http.Post(url, "application/json", buffer)
	if err != nil {
		return errors.New(err.Error() + ": " + url + ": " + bufferStr)
		//		return err
	}
	defer resp.Body.Close()
	// Do this way in case we get 201
	if resp.StatusCode/100 != 2 {
		return errors.New(resp.Status + ": " + url + ": " + bufferStr)
		// return errors.New(resp.Status)
	}
	return nil
}
