#!/bin/sh
# Regenerates the test-only certificates in this directory: a CA, a server certificate for
# localhost and 127.0.0.1, and a client certificate, all P-256 and valid for 100 years. The
# CA's key is discarded, so nothing else can be signed with it.
#
# Usage: tools/fixtures/tls/generate.sh
set -eu
cd "$(dirname "$0")"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
days=36500

key() { openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1"; }

key ca.key
cat > "$tmp/ca.ext" <<'EXT'
basicConstraints = critical, CA:true
keyUsage = critical, keyCertSign, cRLSign
subjectKeyIdentifier = hash
EXT
openssl req -new -key ca.key -subj "/CN=temporal-sdk-zig samples test CA" -out "$tmp/ca.csr"
openssl x509 -req -in "$tmp/ca.csr" -signkey ca.key -days "$days" -sha256 -extfile "$tmp/ca.ext" -out ca.pem

leaf() {
    name=$1 cn=$2 usage=$3 san=$4
    key "$name.key"
    {
        echo "basicConstraints = critical, CA:false"
        echo "keyUsage = critical, digitalSignature"
        echo "extendedKeyUsage = $usage"
        echo "authorityKeyIdentifier = keyid"
        if [ -n "$san" ]; then echo "subjectAltName = $san"; fi
    } > "$tmp/$name.ext"
    openssl req -new -key "$name.key" -subj "/CN=$cn" -out "$tmp/$name.csr"
    openssl x509 -req -in "$tmp/$name.csr" -CA ca.pem -CAkey ca.key -set_serial "0x$(openssl rand -hex 16)" \
        -days "$days" -sha256 -extfile "$tmp/$name.ext" -out "$name.pem"
}

leaf server localhost serverAuth "DNS:localhost, IP:127.0.0.1"
leaf client samples-client clientAuth ""
rm ca.key
# The server reads its key as uid 1000 inside its container.
chmod 644 ./*.key
