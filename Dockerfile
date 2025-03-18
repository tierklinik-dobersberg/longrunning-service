
# Build the gobinary

FROM golang:1.23 AS gobuild

RUN update-ca-certificates

WORKDIR /go/src/app

COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download
RUN go mod verify

COPY ./ ./

RUN CGO_ENABLED=0 go build -o /go/bin/longrunning-service ./cmds/service

FROM alpine:latest

COPY --from=gobuild /go/bin/longrunning-service /go/bin/longrunning-service
EXPOSE 8081

ENTRYPOINT ["/go/bin/longrunning-service"]
