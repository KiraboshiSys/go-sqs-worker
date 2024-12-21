.PHONY: \
	install-deps \
	gen \
	lint \

install-deps: ## Install dependencies
	go mod tidy
	go install honnef.co/go/tools/cmd/staticcheck@latest
	go install go.uber.org/mock/mockgen@latest
	go install golang.org/x/tools/cmd/stringer@latest

gen: ## Execute Go generate commands to process source code annotations
	find ./ -type d -name "mock_*" -exec rm -r {} +
	go generate ./...

lint: ## Run linters (vet and staticcheck)
	go vet ./...
	staticcheck ./...
