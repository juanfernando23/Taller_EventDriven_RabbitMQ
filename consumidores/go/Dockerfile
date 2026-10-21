# go.sum se genera dentro del build (go mod tidy) porque Go no está instalado en el host
FROM golang:1.24-alpine AS build

WORKDIR /src

COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/consumidor .

FROM alpine:3.22

COPY --from=build /out/consumidor /usr/local/bin/consumidor

CMD ["consumidor"]
