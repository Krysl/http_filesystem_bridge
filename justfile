default:
  @just --choose

about:
    @awk '/^#/ {print} !/^#/ {exit}' "{{justfile()}}"
    @echo 'Summarises the purpose of this file.'
    @echo 'This lists the comment lines of the file until the first line that does not start with a '#' character. Then it lists the targets of the file.'
    @just --list

alias s := server
alias r := run

server:
  #!/bin/sh
  cd ../../TabTree/
  flutter run \
    -d chrome \
    --wasm \
    --web-browser-flag \
    --disable-web-security \
    --no-web-resources-cdn \
    --web-port=5223 \
    --web-launch-url=http://localhost:5223/index.html

run:
  export RUST_LOG=info \
    && cargo run -- \
      --mount-point Z: \
      -u http://localhost:5223 \
      -j tests/tree.json \
      -i