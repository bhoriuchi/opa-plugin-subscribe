FROM golang:1.16-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY *.go ./
COPY subscribe/ ./subscribe/

RUN go build -o /app/opa

EXPOSE 8181

ENTRYPOINT [ "/app/opa" ]
CMD ["run"]