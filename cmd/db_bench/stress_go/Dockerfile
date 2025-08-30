FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/go-stress ./cmd/db_bench/stress_go

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=build /out/go-stress /app/go-stress
USER nonroot
ENTRYPOINT ["/app/go-stress"]