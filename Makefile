test:
	go test -v -cpu=2 -bench=./... ./tests/...
	go test -v -coverprofile=c.out -coverpkg ./... ./tests/...
	go tool cover -html=c.out -o coverage.html

.PHONY: test