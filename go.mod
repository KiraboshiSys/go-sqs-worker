module github.com/mickamy/go-sqs-worker

go 1.24.0

require (
	github.com/alicebob/miniredis/v2 v2.31.1
	github.com/aws/aws-sdk-go-v2 v1.39.4
	github.com/aws/aws-sdk-go-v2/service/scheduler v1.17.7
	github.com/aws/aws-sdk-go-v2/service/sqs v1.42.11
	github.com/go-playground/validator/v10 v10.28.0
	github.com/google/uuid v1.6.0
	github.com/redis/go-redis/v9 v9.16.0
	github.com/stretchr/testify v1.11.1
	go.uber.org/mock v0.6.0
)

require (
	github.com/BurntSushi/toml v1.4.1-0.20240526193622-a339e1f7089c // indirect
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.11 // indirect
	github.com/aws/smithy-go v1.23.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gabriel-vasile/mimetype v1.4.10 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/yuin/gopher-lua v1.1.0 // indirect
	golang.org/x/crypto v0.43.0 // indirect
	golang.org/x/exp/typeparams v0.0.0-20231108232855-2478ac86f678 // indirect
	golang.org/x/mod v0.29.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
	golang.org/x/text v0.30.0 // indirect
	golang.org/x/tools v0.38.0 // indirect
	golang.org/x/tools/go/expect v0.1.1-deprecated // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	honnef.co/go/tools v0.6.1 // indirect
)

tool (
	go.uber.org/mock/mockgen
	golang.org/x/tools/cmd/stringer
	honnef.co/go/tools/cmd/staticcheck
)
