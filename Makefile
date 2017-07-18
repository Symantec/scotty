get-deps: ${GOPATH}/bin/gdm
	go get -t -d ./...
	${GOPATH}/bin/gdm restore

${GOPATH}/bin/gdm:
	go get github.com/sparrc/gdm

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
