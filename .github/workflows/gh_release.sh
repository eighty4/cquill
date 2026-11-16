#!/bin/bash
set -e

: "${GH_TOKEN:?GH_TOKEN must be set}"
: "${RELEASE_NAME:?RELEASE_NAME must be set}"
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

_assets_path="$1"

if [ ! -d "$_assets_path" ]; then
    echo "error: $_assets_path is not a directory" >&2
    exit 1
fi

if [ -z "$(ls "$_assets_path")" ]; then
    echo "error: $_assets_path is an empty directory" >&2
    exit 1
fi

for _asset_path in "$_assets_path"/*; do
    # skip iteration over `*` when directory is empty
    # this is a shell kinda thing ¯\_(ツ)_/¯
    [ ! -e "$_asset_path" ] && continue

    # error on directories, pipes, sockets
    if [ ! -f "$_asset_path" ]; then
        echo "error: $_asset_path is not a regular file" >&2
        exit 1
    fi

    # error if mime type is unexpected
    _mime_type=$(file -bE --mime-type "$_asset_path")
    case "$_mime_type" in
        application/vnd.microsoft.portable-executable|\
        application/x-mach-binary|\
        application/x-pie-executable)
            ;;
        *)
            echo "error: $_asset_path has invalid mime type $_mime_type." >&2
            readelf --file-header "$_asset_path"
            exit 1
            ;;
    esac
done

if ! command -v "changelog" &> /dev/null; then
  echo "error: \`npm i -g @eighty4/changelog\` is required" >&2
fi

if ! command -v "curl" &> /dev/null; then
  echo "error: \`curl\` is required" >&2
fi

if ! command -v "gh" &> /dev/null; then
  echo "error: \`gh\` is required" >&2
fi

case "$PRERELEASE" in
    true) _changelog_tag="Unreleased"; _install_tag="@$VERSION" ;;
    false) _changelog_tag="v$VERSION"; _install_tag="" ;;
esac

_crates_url="https://crates.io/crates/cquill/$VERSION"
_release_notes=()
_release_notes+=("#### Published on crates.io as [cquill]($_crates_url)")
_release_notes+=('```shell')
_release_notes+=("cargo install cquill$_install_tag")
_release_notes+=('```')
_release_notes+=("## Release notes")
mapfile -t -O "${#_release_notes[@]}" _release_notes < <(changelog get "$_changelog_tag" --changelog-file cquill/CHANGELOG.md)

_created_release=$(printf '%s\n' "${_release_notes[@]}" | gh api \
  --method POST \
  -H "Accept: application/vnd.github+json" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  /repos/eighty4/cquill/releases \
  -F body=@- \
  -f tag_name="$TAG_NAME" \
  -f name="$RELEASE_NAME" \
  -F draft=false \
  -f make_latest="$LATEST" \
  -F prerelease="$PRERELEASE" \
  -F generate_release_notes=false)

_release_id=$(echo $_created_release | jq '.id')
_upload_hostname=$(echo $_created_release | jq '.upload_url' | cut -d'/' -f3)

for _asset_path in "$_assets_path"/*; do
    if [ -f "$_asset_path" ]; then
        _filename=$(basename "$_asset_path")
        echo "uploading $_filename from $_assets_path to release $_release_id"
        curl --fail --silent -L -X POST \
          -H "Accept: application/vnd.github+json" \
          -H "Authorization: Bearer $GH_TOKEN" \
          -H "X-GitHub-Api-Version: 2022-11-28" \
          -H "Content-Type: $(file $_asset_path -b --mime-type)" \
          "https://$_upload_hostname/repos/eighty4/cquill/releases/$_release_id/assets?name=$_filename" \
          --data-binary "@$_asset_path"
    fi
done
