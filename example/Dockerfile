ARG GO_VERSION=1.23.4
FROM golang:${GO_VERSION}
WORKDIR /src

ADD go.* .

RUN go mod tidy

ADD . .
