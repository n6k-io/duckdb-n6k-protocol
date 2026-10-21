#!/bin/bash

# Extension upload script

# Usage: ./extension-upload.sh <name> <extension_version> <duckdb_version> <architecture> <s3_bucket> <copy_to_latest> <copy_to_versioned>
# <name>                : Name of the extension
# <extension_version>   : Version (commit / version tag) of the extension
# <duckdb_version>      : Version (commit / version tag) of DuckDB
# <architecture>        : Architecture target of the extension binary
# <s3_bucket>           : S3 bucket to upload to
# <copy_to_latest>      : Set this as the latest version ("true" / "false", default: "false")
# <copy_to_versioned>   : Set this as a versioned version that will prevent its deletion

set -e

if [[ $4 == wasm* ]]; then
  ext="/tmp/extension/$1.duckdb_extension.wasm"
else
  ext="/tmp/extension/$1.duckdb_extension"
fi

echo $ext

script_dir="$(dirname "$(readlink -f "$0")")"

cat $ext > $ext.append

# The build appends a 256-byte zero placeholder for the signature as the last
# bytes of the file (see duckdb/scripts/append_metadata.cmake). Strip it before
# computing the hash and appending the real signature so the total footer
# length stays the same and the metadata block remains in DuckDB's read window.
( command -v truncate && truncate -s -256 $ext.append ) || ( command -v gtruncate && gtruncate -s -256 $ext.append ) || exit 1

# (Optionally) Sign binary
if [ "$DUCKDB_EXTENSION_SIGNING_PK" != "" ]; then
  echo "$DUCKDB_EXTENSION_SIGNING_PK" > private.pem
  $script_dir/../duckdb/scripts/compute-extension-hash.sh $ext.append > $ext.hash
  openssl pkeyutl -sign -in $ext.hash -inkey private.pem -pkeyopt digest:sha256 -out $ext.sign
  rm -f private.pem
else
  # No signing key: write a 256-byte zero signature.
  dd if=/dev/zero of=$ext.sign bs=256 count=1
fi

cat $ext.sign >> $ext.append
rm $ext.sign

# compress extension binary
if [[ $4 == wasm_* ]]; then
  brotli < $ext.append > "$ext.compressed"
else
  gzip < $ext.append > "$ext.compressed"
fi

set -e

# upload versioned version
if [[ $7 = 'true' ]]; then
  if [[ $4 == wasm* ]]; then
    gcloud storage cp $ext.compressed "gs://$5/$1/$2/$3/$4/$1.duckdb_extension.wasm" --content-encoding=br --content-type=application/wasm
  else
    gcloud storage cp $ext.compressed "gs://$5/$1/$2/$3/$4/$1.duckdb_extension.gz"
  fi
fi

# upload to latest version
if [[ $6 = 'true' ]]; then
  if [[ $4 == wasm* ]]; then
    gcloud storage cp $ext.compressed "gs://$5/$3/$4/$1.duckdb_extension.wasm" --content-encoding=br --content-type=application/wasm
  else
    gcloud storage cp $ext.compressed "gs://$5/$3/$4/$1.duckdb_extension.gz"
  fi
fi
