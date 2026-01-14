FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod ./
RUN go get espg
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/espg .

FROM alpine:3.20
RUN adduser -D -g '' app
USER app
WORKDIR /home/app
COPY --from=build /bin/espg /usr/local/bin/espg
EXPOSE 3000
ENV PORT=3000
ENTRYPOINT ["espg"]
