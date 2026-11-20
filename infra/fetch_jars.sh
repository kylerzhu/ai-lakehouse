#!/bin/sh
# 镜像构建时使用：从 jars.txt 中挑出指定目标（flink / spark）的 jar，下载到目标目录，
# 并用 Maven Central 提供的 .sha1 校验完整性，任何一个失败都会让构建失败。
#
# 用法：fetch_jars.sh <flink|spark> <目标目录> [jars.txt 路径]
set -eu

target="$1"
dest="$2"
list="${3:-/tmp/jars.txt}"
maven="${MAVEN_MIRROR:-https://repo1.maven.org/maven2}"

mkdir -p "${dest}"
count=0
while read -r t path; do
  case "${t}" in ''|\#*) continue ;; esac
  [ "${t}" = "${target}" ] || continue
  name="$(basename "${path}")"
  wget -q -O "${dest}/${name}" "${maven}/${path}"
  expected="$(wget -q -O - "${maven}/${path}.sha1" | cut -c1-40)"
  actual="$(sha1sum "${dest}/${name}" | cut -c1-40)"
  if [ "${expected}" != "${actual}" ]; then
    echo "sha1 校验失败：${name}（期望 ${expected}，实际 ${actual}）" >&2
    exit 1
  fi
  echo "ok  ${name}"
  count=$((count + 1))
done < "${list}"

if [ "${count}" -eq 0 ]; then
  echo "jars.txt 中没有目标为 ${target} 的 jar" >&2
  exit 1
fi
