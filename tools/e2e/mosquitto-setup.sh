#!/bin/sh
# Writes a throw-away Mosquitto 2 configuration for tools/e2e/mqtt_smoke.py:
# anonymous access off, one user in a password file, plain MQTT on 1883 and
# MQTT over TLS on 8883 with a self-signed test CA.
#
#   tools/e2e/mosquitto-setup.sh DIR USER PASSWORD [EXTRA_IP]
#   docker run -d --name ferrika-mqtt-test -p 1883:1883 -p 8883:8883 \
#       -v DIR:/mosquitto/config eclipse-mosquitto:2
#
# DIR/ca.pem is the CA to pass to the smoke test (--ca-file). The server
# certificate is valid for localhost, 127.0.0.1 and EXTRA_IP (e.g. the
# address of a WSL or VM host), for two days. Needs openssl and docker.
# Test brokers only: the keys are not protected.
set -eu

DIR=$1
USER=$2
PASS=$3
EXTRA_IP=${4:-}

mkdir -p "$DIR"
cd "$DIR"

cat > mosquitto.conf <<'CONF'
per_listener_settings false
allow_anonymous false
password_file /mosquitto/config/passwd

listener 1883

listener 8883
cafile /mosquitto/config/ca.pem
certfile /mosquitto/config/server.pem
keyfile /mosquitto/config/server.key
CONF

SAN="DNS:localhost,IP:127.0.0.1"
if [ -n "$EXTRA_IP" ]; then
    SAN="$SAN,IP:$EXTRA_IP"
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=ferrika-e2e-test-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout ca.key -out ca.pem 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj "/CN=localhost" \
    -keyout server.key -out server.csr 2>/dev/null
printf 'subjectAltName=%s\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE\n' "$SAN" > server.ext
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 \
    -extfile server.ext -out server.pem 2>/dev/null
rm -f server.csr server.ext ca.srl
# The broker reads the key as the mosquitto user.
chmod 644 server.key

# The password file, hashed by the image's own mosquitto_passwd and owned
# by the broker's user (it refuses files others can read).
docker run --rm -v "$DIR:/mosquitto/config" -e "U=$USER" -e "P=$PASS" eclipse-mosquitto:2 \
    sh -c 'mosquitto_passwd -b -c /mosquitto/config/passwd "$U" "$P" &&
        chown mosquitto:mosquitto /mosquitto/config/passwd &&
        chmod 0600 /mosquitto/config/passwd'
echo "Mosquitto configuration written to $DIR (CA: $DIR/ca.pem)"
