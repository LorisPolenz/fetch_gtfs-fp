FROM golang:1.25.3 AS builder

WORKDIR /go/src

COPY . .

RUN GOOS=linux go build -o /go/bin/app

FROM golang:1.25.3

COPY --from=builder /go/bin/app /

ENTRYPOINT ["/app"]