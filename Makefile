.PHONY: \
	up \
	down \
	build \
	install-deps \
	gen \
	lint \
	test \

up: down ## Start the Docker containers
	docker compose up

down: ## Stop the Docker containers
	docker compose down

build: ## Build the Docker images
	docker compose build

install: ## Install dependencies
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

test: ## Run tests locally on the host machine
	go test ./...

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
