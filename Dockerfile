FROM golang:1.22-alpine AS build

WORKDIR /src

# 先拷贝 go.mod 以利用缓存
COPY go.mod ./
RUN go env -w GOPROXY=https://proxy.golang.org,direct
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/server ./cmd/server

FROM alpine:3.19
WORKDIR /app
COPY --from=build /out/server /app/server
EXPOSE 8080
ENTRYPOINT ["/app/server"]
