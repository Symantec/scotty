getdeps:
	go get github.com/sparrc/gdm
	go get github.com/influxdata/influxdb
	cd $(GOPATH)/src/github.com/influxdata/influxdb; $(GOPATH)/bin/gdm restore
	go get -t github.com/Symantec/scotty/...

all:
	@cd $(GOPATH)/src; go install github.com/Symantec/scotty/...

scotty.tarball:
	@./scripts/make-tarball.sh

format:
	gofmt -s -w .

test:
	@find * -name '*_test.go' |\
	sed -e 's@^@github.com/Symantec/scotty/@' -e 's@/[^/]*$$@@' |\
	sort -u | xargs go test

