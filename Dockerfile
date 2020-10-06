FROM alpine:3.12.0
RUN apk add --no-cache sqlite-dev mariadb-connector-c libpq

ARG VERSION=0.5.0-beta.1

ADD https://github.com/rnovales/teleport/releases/download/v${VERSION}/teleport_${VERSION}.linux-x86_64.tar.gz /tmp/
RUN tar xzvf /tmp/teleport_${VERSION}.linux-x86_64.tar.gz teleport_${VERSION}.linux-x86_64/teleport
RUN mv /teleport_${VERSION}.linux-x86_64/teleport /teleport
RUN rmdir /teleport_${VERSION}.linux-x86_64/
RUN rm /tmp/teleport_${VERSION}.linux-x86_64.tar.gz

RUN mkdir /pad

ENV PADPATH "/pad"
ENTRYPOINT ["/teleport"]
CMD ["version"]
