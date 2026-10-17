{
  description = "Toudai Collector";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    # Toudai pins ClickHouse 26.8.2.7-lts for its benchmarks and tests. This is
    # the same nixpkgs revision it uses, pinned separately for ClickHouse only so
    # the Go toolchain can follow nixos-unstable.
    nixpkgs-clickhouse.url = "github:nixos/nixpkgs/8ce4ef6cb6f871616146b9fe26d2a5ae594e94fe";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-clickhouse,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        clickhousePkgs = import nixpkgs-clickhouse {
          inherit system;
          config.allowUnfree = true;
        };
      in
      {
        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            go
            gopls
            golangci-lint
            git-cliff
            gnumake

            # Local ClickHouse for exercising the exporter against the Toudai DDL.
            clickhousePkgs.clickhouse-lts
          ];
        };
      }
    );
}
