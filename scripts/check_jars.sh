#!/usr/bin/env bash
# 验证 infra/jars.txt 中每个 jar 都能从 Maven Central 下载（HTTP 200）。
set -euo pipefail
MAVEN="${MAVEN_MIRROR:-https://repo1.maven.org/maven2}"
cd "$(dirname "$0")/.."
fail=0
while read -r target path; do
  [[ -z "${target}" || "${target}" == \#* ]] && continue
  code=$(curl -s -o /dev/null -w '%{http_code}' -I "${MAVEN}/${path}")
  size=$(curl -sI "${MAVEN}/${path}" | awk 'tolower($1)=="content-length:"{printf "%.1f MB", $2/1048576}')
  printf '%-6s %-4s %-10s %s\n' "${target}" "${code}" "${size}" "$(basename "${path}")"
  [[ "${code}" == "200" ]] || fail=1
done < infra/jars.txt
exit "${fail}"
