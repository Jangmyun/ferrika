#!/bin/sh
# Writes a throw-away ksqlDB 0.29 server configuration for the HTTPS,
# basic-auth and mutual-TLS checks of tools/e2e/ksql_smoke.py: a test CA, a
# server certificate (PKCS12 keystore), a client certificate and key (PEM),
# a truststore holding the CA (so the server requires client certificates
# from it), and a JAAS file with one user for HTTP basic authentication.
#
#   tools/e2e/ksqldb-tls-setup.sh DIR USER PASSWORD [EXTRA_IP]
#   docker run -d --name ferrika-ksql-tls-test --network host \
#       -v DIR:/etc/ksqldb-e2e --env-file DIR/ksqldb.env \
#       confluentinc/ksqldb-server:0.29.0
#
# The server listens on https://0.0.0.0:8089 (KSQL_LISTENERS in
# DIR/ksqldb.env; edit it for another port) and reaches Kafka on
# localhost:9092 (KSQL_BOOTSTRAP_SERVERS). Pass the smoke test DIR/ca.pem
# (--tls-ca), DIR/client.pem and DIR/client.key (--tls-cert, --tls-key).
# The server certificate is valid for localhost, 127.0.0.1 and EXTRA_IP,
# for two days. Needs openssl and docker (the truststore is written with
# the ksqlDB image's own keytool, since a Java truststore needs entries
# openssl cannot mark as trusted). Test servers only: the keys are not
# protected and the store password is fixed.
set -eu

DIR=$1
USER=$2
PASS=$3
EXTRA_IP=${4:-}
IMAGE=${KSQLDB_IMAGE:-confluentinc/ksqldb-server:0.29.0}
STORE_PASS=ferrika-e2e-store

mkdir -p "$DIR"
cd "$DIR"

SAN="DNS:localhost,IP:127.0.0.1"
if [ -n "$EXTRA_IP" ]; then
    SAN="$SAN,IP:$EXTRA_IP"
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=ferrika-e2e-ksql-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout ca.key -out ca.pem 2>/dev/null

# Server: certificate for localhost, in a PKCS12 keystore.
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' "$SAN" > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile server.ext -out server.pem 2>/dev/null
openssl pkcs12 -export -in server.pem -inkey server.key -certfile ca.pem -name server \
    -passout "pass:$STORE_PASS" -out server.p12

# Client: certificate and unencrypted key in PEM, as Ferrika reads them.
openssl req -newkey rsa:2048 -nodes -subj "/CN=ferrika-e2e-client" -keyout client.key -out client.csr 2>/dev/null
printf 'extendedKeyUsage=clientAuth\nbasicConstraints=CA:FALSE\n' > client.ext
openssl x509 -req -in client.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile client.ext -out client.pem 2>/dev/null
rm -f server.csr server.ext client.csr client.ext ca.srl

# The truststore the server checks client certificates against.
rm -f truststore.p12
docker run --rm -v "$PWD:/w" --entrypoint keytool "$IMAGE" -importcert -noprompt \
    -alias ferrika-e2e-ca -file /w/ca.pem -keystore /w/truststore.p12 -storetype PKCS12 \
    -storepass "$STORE_PASS" >/dev/null

# HTTP basic authentication: one user with the role the server admits.
printf '%s: %s,admin\n' "$USER" "$PASS" > password.properties
cat > jaas.conf <<'JAAS'
KsqlServer-Props {
  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required
  file="/etc/ksqldb-e2e/password.properties"
  debug="false";
};
JAAS

cat > ksqldb.env <<ENV
KSQL_BOOTSTRAP_SERVERS=localhost:9092
KSQL_KSQL_SERVICE_ID=ferrika_e2e_tls_
KSQL_LISTENERS=https://0.0.0.0:8089
KSQL_SSL_KEYSTORE_LOCATION=/etc/ksqldb-e2e/server.p12
KSQL_SSL_KEYSTORE_TYPE=PKCS12
KSQL_SSL_KEYSTORE_PASSWORD=$STORE_PASS
KSQL_SSL_KEY_PASSWORD=$STORE_PASS
KSQL_SSL_TRUSTSTORE_LOCATION=/etc/ksqldb-e2e/truststore.p12
KSQL_SSL_TRUSTSTORE_TYPE=PKCS12
KSQL_SSL_TRUSTSTORE_PASSWORD=$STORE_PASS
KSQL_SSL_CLIENT_AUTHENTICATION=REQUIRED
KSQL_AUTHENTICATION_METHOD=BASIC
KSQL_AUTHENTICATION_REALM=KsqlServer-Props
KSQL_AUTHENTICATION_ROLES=admin
KSQL_OPTS=-Djava.security.auth.login.config=/etc/ksqldb-e2e/jaas.conf
ENV

# The server runs as an unprivileged user.
chmod 644 ./*
echo "ksqlDB TLS configuration written to $DIR (CA: $DIR/ca.pem, client: $DIR/client.pem, $DIR/client.key)"
