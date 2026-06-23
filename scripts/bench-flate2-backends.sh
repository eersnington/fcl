#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s [--runs N] [--out DIR] [--backend NAME]... [--no-git] <repo-url>...\n' "$0" >&2
}

runs=1
out_dir="target/fcl-backend-bench"
compare_git=1
backends=()
repos=()

while (($#)); do
  case "$1" in
    --runs)
      runs="${2:?--runs requires a value}"
      shift 2
      ;;
    --out)
      out_dir="${2:?--out requires a value}"
      shift 2
      ;;
    --backend)
      backends+=("${2:?--backend requires a value}")
      shift 2
      ;;
    --no-git)
      compare_git=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      usage
      exit 2
      ;;
    *)
      repos+=("$1")
      shift
      ;;
  esac
done

if ((${#repos[@]} == 0)); then
  usage
  exit 2
fi

if ((${#backends[@]} == 0)); then
  backends=(
    flate2-rust
    flate2-miniz-oxide
    flate2-zlib-rs
    flate2-zlib-ng
    flate2-zlib-ng-compat
    flate2-zlib
  )
fi

mkdir -p "$out_dir"
combined="$out_dir/combined.csv"
rm -f "$combined"

csv_header_written=0
for backend in "${backends[@]}"; do
  printf '==> building backend %s\n' "$backend" >&2
  if ! cargo build -p fcl --release --no-default-features --features "$backend"; then
    printf 'backend %s: build failed on this host\n' "$backend" | tee "$out_dir/$backend.unsupported" >&2
    continue
  fi

  for repo in "${repos[@]}"; do
    safe_repo=$(printf '%s' "$repo" | tr -c '[:alnum:]._-' '_')
    for mode in sequential pipeline; do
      output="$out_dir/$backend.$mode.$safe_repo.csv"
      args=(bench "$repo" --runs "$runs" --order alternate --git-trace2 --validate --csv)
      if ((compare_git)); then
        args+=(--compare-git)
      fi
      printf '==> backend=%s mode=%s repo=%s\n' "$backend" "$mode" "$repo" >&2
      if [[ "$mode" == pipeline ]]; then
        target/release/fcl "${args[@]}" > "$output"
      else
        target/release/fcl "${args[@]}" --no-pipeline > "$output"
      fi
      if ((csv_header_written == 0)); then
        sed -n '1p' "$output" > "$combined"
        csv_header_written=1
      fi
      sed -n '2,$p' "$output" >> "$combined"
    done
  done
done

printf 'combined results: %s\n' "$combined" >&2
