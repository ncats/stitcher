#!/usr/bin/env bash

pkeysrc="/etc/letsencrypt/live/disease.ncats.io/privkey.pem"
certsrc="/etc/letsencrypt/live/disease.ncats.io/fullchain.pem"
neo4j="/data/neo4j-community-3.5.18"
pkeydst="$neo4j/certificates/neo4j.key"
certdst="$neo4j/certificates/neo4j.cert"

update() {
    echo "updating certificates..."
    cp $pkeysrc $pkeydst
    cp $certsrc $certdst
    ulimit -n 40000
    $neo4j/bin/neo4j restart
}

test_update() {
	echo "updating certificates..."
}

diff $pkeysrc $pkeydst 2>&1 >/dev/null || update
