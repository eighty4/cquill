#!/bin/bash
set -e

: "${GH_TOKEN:?GH_TOKEN must be set}"
: "${TAG_NAME:?TAG_NAME must be set}"
: "${VERSION:?VERSION must be set}"

for _val in "$LATEST" "$PRERELEASE"; do
    case "$_val" in
        true|false) ;;
        *) echo "LATEST and PRERELEASE must be true/false" >&2; exit 1 ;;
    esac
done

if [ -z "$1" ]; then
    echo "error: \`gh_release.sh BINS_DIR\` is required" >&2
    exit 1
fi

_artifacts_dir="$1"

if ! command -v "changelog" &> /dev/null; then
  echo "error: \`npm i -g @eighty4/changelog\` is required"
fi

if ! command -v "curl" &> /dev/null; then
  echo "error: \`curl\` is required"
fi

if ! command -v "gh" &> /dev/null; then
  echo "error: \`gh\` is required"
fi

case "$PRERELEASE" in
    true) _changelog_tag="Unreleased" ;;
    false) _changelog_tag="v$VERSION" ;;
esac

_changelog=$(changelog get "$_changelog_tag" --changelog-file cquill/CHANGELOG.md)
_crates_url="https://crates.io/crates/cquill/$VERSION"
_release_notes="[Published on crates.io]($_crates_url)\r\n\r\n## Release notes\r\n\r\n$_changelog"
_created_release=$(echo "$_release_notes" | gh api \
  --method POST \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  /repos/eighty4/cquill/releases \
  -f tag_name="$TAG_NAME" \
  -f name="v$VERSION" \
  -F draft=false \
  -f make_latest="$LATEST" \
  -F prerelease="$PRERELEASE" \
  -F generate_release_notes=false)

_release_id=$(echo $_created_release | jq '.id')
_upload_hostname=$(echo $_created_release | jq '.upload_url' | cut -d'/' -f3)

for _filename in "$_artifacts_dir"/*; do
    if [ -f "$_filename" ]; then
        curl --fail --silent -L -X POST \
          -H "Accept: application/vnd.github+json" \
          -H "Authorization: Bearer $GH_TOKEN" \
          -H "X-GitHub-Api-Version: 2022-11-28" \
          -H "Content-Type: $(file $_filename -b --mime-type)" \
          "https://$_upload_hostname/repos/eighty4/cquill/releases/$_release_id/assets?name=$_filename" \
          --data-binary "@$_filename"
    fi
done
