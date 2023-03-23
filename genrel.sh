#!/usr/bin/env bash
# -*- coding: utf-8 -*-

# jq is required to parse the version number
if ! command -v jq >/dev/null 2>&1; then
	echo "jq is required to parse the version number"
	exit 1
fi

# We need zip to make the release distribution
if ! command -v zip >/dev/null 2>&1; then
	echo "zip is required to make the release distribution"
	exit 1
fi

# We need git to make the source distribution
if ! command -v git >/dev/null 2>&1; then
	echo "git is required to make the source distribution"
	exit 1
fi

# s3cmd is required to upload the release
if ! command -v s3cmd >/dev/null 2>&1; then
	echo "s3cmd is required to upload the release"
	exit 1
fi

# Make sure there is a Cargo.toml file
if [ ! -f Cargo.toml ]; then
	echo "Cargo.toml not found"
	exit 1
fi

# List the triplets to build for
TARGETS="x86_64-unknown-linux-musl x86_64-unknown-linux-gnu x86_64-pc-windows-gnu i686-pc-windows-gnu"

# Make the release distribution
VER=$(cargo metadata --no-deps --offline --format-version 1 | jq '.packages[0].version' -r)
NAME=$(cargo metadata --no-deps --offline --format-version 1 | jq '.packages[0].name' -r)

# S3 Configuration
S3BUCKET="neonlayer"
SETACL="public-read"

# Clean any previous build
cargo clean
rm -rf dist

# Make the release directory
mkdir -p dist
echo "$VER" > dist/latest.txt

# Build the release for each target
for TARGET in $TARGETS; do
	# is this a windows target?
	if [[ "$TARGET" == *windows* ]]; then
		BINNAME="$NAME.exe"
	else # assume it's a unix target
		BINNAME="$NAME"
	fi

	echo "Building $NAME for $TARGET"
	cargo build --release --target "$TARGET"
	mkdir -p "dist/$TARGET"
	cp "target/$TARGET/release/$BINNAME" "dist/$TARGET"
	zip -j "dist/$NAME-$TARGET.zip" "dist/$TARGET/$BINNAME" "dist/latest.txt" "LICENSE" "readme.md"
	rm -rf "dist/$TARGET"
	echo "$TARGET $NAME-$TARGET.zip" >> dist/manifest.txt
done

# Make the source distribution
echo "Making source distribution"
git archive --format zip --output "dist/$NAME-$VER.zip" HEAD
git archive --format tar.gz --output "dist/$NAME-$VER.tar.gz" HEAD

# Make the checksums
echo "Making checksums"
cd dist || exit 1
# for each file in the dist directory
for FILE in *; do
	# make the checksum
	sha256sum "$FILE" > "$FILE.sha256"
	cat "$FILE.sha256" >> sha256sums
	# Sign the checksums
	gpg --detach-sign --armor "$FILE.sha256"
done
cd .. || exit 1

echo "Done building release, dist is ready for upload"

# Upload the release
echo "Uploading release"
if [ -z "$S3BUCKET" ]; then
	echo "S3BUCKET is not set"
	exit 1
fi

s3cmd put --recursive dist/ "s3://$S3BUCKET/releases/$NAME/"

# Set the ACL on the files if SETACL is set
if [ -n "$SETACL" ]; then
	s3cmd setacl --acl-public --recursive "s3://$S3BUCKET/releases/$NAME/"
fi

echo "Done uploading release"

echo "All done!"
