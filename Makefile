.PHONY: tikv-copier

tikv-copier:
	CGO_ENABLED=0 GO111MODULE=on go build -trimpath -o tikv-copier main.go
