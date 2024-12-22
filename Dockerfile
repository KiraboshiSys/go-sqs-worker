ARG GO_VERSION=1.23.4
FROM golang:${GO_VERSION}

WORKDIR /src/example
COPY ./example/go.* .
RUN go mod tidy

WORKDIR /src
COPY . .
