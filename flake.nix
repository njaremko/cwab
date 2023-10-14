{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.05";
    flake-utils.url = "github:numtide/flake-utils";
    nix-filter.url = "github:numtide/nix-filter";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
      };
    };
    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, nix-filter, rust-overlay, crane, advisory-db, flake-utils }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [
            (import rust-overlay)
            (final: prev: {
              nix-filter = nix-filter.lib;
              rust-toolchain = final.rust-bin.stable.latest.default;
              rust-dev-toolchain = final.rust-toolchain.override {
                extensions = [ "rust-src" ];
              };
            })
          ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          craneLib =
            (crane.mkLib pkgs).overrideToolchain pkgs.rust-toolchain;
          lib = pkgs.lib;
          stdenv = pkgs.stdenv;
          commonNativeBuildInputs = with pkgs; [
            openssl
            pkg-config
          ] ++ lib.lists.optionals pkgs.stdenv.isDarwin [ darwin.apple_sdk.frameworks.Security ];
          src = craneLib.cleanCargoSource (craneLib.path ./.);
          commonArgs = {
            inherit src;
            nativeBuildInputs = commonNativeBuildInputs;
          };
          # Build *just* the cargo dependencies, so we can reuse
          # all of that work (e.g. via cachix) when running in CI
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
          cwab = craneLib.buildPackage (commonArgs // {
            inherit cargoArtifacts;
          });
        in
        rec {
          # `nix build`
          packages.default = cwab;

          # `nix develop`
          devShells.default = import ./shell.nix { inherit pkgs; };

          checks = {
            # Build the crate as part of `nix flake check` for convenience
            inherit cwab;

            # Run clippy (and deny all warnings) on the crate source,
            # again, resuing the dependency artifacts from above.
            #
            # Note that this is done as a separate derivation so that
            # we can block the CI if there are issues here, but not
            # prevent downstream consumers from building our crate by itself.
            cwab-clippy = craneLib.cargoClippy (commonArgs // {
              inherit cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets --  --deny warnings";
            });

            cwab-doc = craneLib.cargoDoc (commonArgs // {
              inherit cargoArtifacts;
            });

            # Check formatting
            cwab-fmt = craneLib.cargoFmt {
              inherit src;
            };

            # Audit dependencies
            cwab-audit = craneLib.cargoAudit {
              inherit src advisory-db;
            };

            # Run tests with cargo-nextest
            # Consider setting `doCheck = false` on `cwab` if you do not want
            # the tests to run twice
            cwab-nextest = craneLib.cargoNextest (commonArgs // {
              inherit cargoArtifacts;
              partitions = 1;
              partitionType = "count";
            });
          };
        });
}
