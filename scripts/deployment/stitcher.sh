nohup ./bin/ncats-stitcher \
  -Dix.version.latest=1 \
  -Dhttp.port=9003 \
  -Dplay.http.secret.key=`head -c2096 /dev/urandom | sha256sum |cut -d' ' -f1` &
