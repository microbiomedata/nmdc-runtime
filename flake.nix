{
  description = "NMDC Runtime";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/master";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
    let
    python = "python39";
    pkgs = nixpkgs.legacyPackages.${system};
    in {
      devShell = pkgs.mkShell {
        buildInputs = [
          (pkgs.${python}.withPackages
            (ps: with ps; [pip python-lsp-server python-lsp-black isort]))
        ];
      };
    });
}
