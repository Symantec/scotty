package nodes

import (
	"encoding/json"
	"net/http"
)

const (
	kUrl = "https://mon-sre.ash2.symcpe.net/machine_data/baremetal.json"
)

func get() (result []string, err error) {
	var client http.Client
	resp, err := client.Get(kUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var content map[string]interface{}
	err = decoder.Decode(&content)
	if err != nil {
		return
	}
	for _, cluster := range content {
		acluster := cluster.(map[string]interface{})
		for _, machine := range acluster {
			amachine := machine.(map[string]interface{})
			name := amachine["fqdn"].(string)
			if name != "" {
				result = append(result, name)
			}
		}
	}
	return
}

func getByCluster(clusterName string) (result []string, err error) {
	var client http.Client
	resp, err := client.Get(kUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var content map[string]interface{}
	err = decoder.Decode(&content)
	if err != nil {
		return
	}
	for cname, cluster := range content {
		if cname != clusterName {
			continue
		}
		acluster := cluster.(map[string]interface{})
		for _, machine := range acluster {
			amachine := machine.(map[string]interface{})
			name := amachine["fqdn"].(string)
			if name != "" {
				result = append(result, name)
			}
		}
	}
	return
}
