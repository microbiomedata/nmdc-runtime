{
  description = "NMDC Runtime";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/master";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
    let
    python = "python310";
    pkgs = nixpkgs.legacyPackages.${system};
    in {
      devShell = pkgs.mkShell {
        buildInputs = [
          (pkgs.${python}.withPackages
            (ps: with ps; [pip python-lsp-server python-lsp-black isort
                           pip-tools pylsp-mypy pydantic mypy]))
          pkgs.gnumake pkgs.ruff pkgs.docker pkgs.docker-compose pkgs.python310
        ];
      };
    });
}
