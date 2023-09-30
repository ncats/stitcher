#!/bin/bash
STITCHER_VERSION="$STITCHER_VERSION"  # Retrieve the environment variable
cp -r $(ls -d /opt/app/stitchv*.db) "/opt/app/db/$STITCHER_VERSION"
mkdir "/opt/app/browsercopy/$STITCHER_VERSION"
mkdir "/opt/app/browsercopy/$STITCHER_VERSION/databases"
mkdir "/opt/app/browsercopy/$STITCHER_VERSION/databases/graph.db"
cp -r $(ls -d /opt/app/stitchv*.db)/* "/opt/app/browsercopy/$STITCHER_VERSION/databases/graph.db/"
exec "$@"
