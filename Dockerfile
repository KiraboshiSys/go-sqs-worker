ARG GO_VERSION=1.24.0
FROM golang:${GO_VERSION}

WORKDIR /src/example
COPY ./example/go.* .
RUN go mod tidy

WORKDIR /src
COPY . .

WORKDIR /src/example
