#! /bin/bash --posix

set -eu

readonly bin="$GOPATH/bin/scotty"
readonly target="/tmp/$LOGNAME/scotty.tar.gz"

(cd $GOPATH/src; go install github.com/Symantec/scotty/...)

mkdir -p ${target%/*}

cd $GOPATH/src/github.com/Symantec/scotty

tar --owner=0 --group=0 -czf $target \
		init.d/scotty.* \
		"$@" \
		-C $PWD/apps/scotty apps.yaml health-check.yml install.sh \
		-C $GOPATH bin/scotty

