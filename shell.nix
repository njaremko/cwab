{ pkgs ? import <nixpkgs> { } }:

with pkgs;

mkShell {
  buildInputs = [
    git
    gnupg
    openssl
    pkg-config
    redis
    nixpkgs-fmt
    rust-dev-toolchain
  ] ++ lib.lists.optionals pkgs.stdenv.isDarwin [ darwin.apple_sdk.frameworks.Foundation ];
}
