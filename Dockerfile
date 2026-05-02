FROM golang:1.25.2-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o container-avahi-publisher .

FROM scratch
COPY --from=builder /app/container-avahi-publisher /container-avahi-publisher
ENTRYPOINT ["/container-avahi-publisher"]
