{ pkgs }:

let
  version = "0.16.0";
  releases = {
    "x86_64-linux" = {
      target = "x86_64-unknown-linux-gnu";
      hash = "sha256-mAAcmVoTTZX5vIMQan+UtVKXG1g/HAq3X7ZWqIHhOGU=";
    };
    "aarch64-linux" = {
      target = "aarch64-unknown-linux-gnu";
      hash = "sha256-h51PDKGn8hpK/G75NFEYuKdaorxKrp5B4EdJlNDvCk8=";
    };
    "x86_64-darwin" = {
      target = "x86_64-apple-darwin";
      hash = "sha256-PZ72IoxO6ybVk8OYstxSUOD21kJZM9spk/zzDUnHi2k=";
    };
    "aarch64-darwin" = {
      target = "aarch64-apple-darwin";
      hash = "sha256-zmVkSRosxLBln0XuF02+8X5N7CTgOpwD0xO1QwvCEJk=";
    };
  };
  release =
    releases.${pkgs.stdenv.hostPlatform.system}
      or (throw "Ruff ${version} is not pinned for ${pkgs.stdenv.hostPlatform.system}");
in
pkgs.stdenvNoCC.mkDerivation {
  pname = "ruff";
  inherit version;

  src = pkgs.fetchurl {
    url = "https://releases.astral.sh/github/ruff/releases/download/${version}/ruff-${release.target}.tar.gz";
    inherit (release) hash;
  };
  sourceRoot = "ruff-${release.target}";

  nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
    pkgs.autoPatchelfHook
  ];
  buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
    pkgs.stdenv.cc.cc.lib
  ];

  installPhase = ''
    runHook preInstall
    install -Dm755 ruff "$out/bin/ruff"
    runHook postInstall
  '';

  nativeInstallCheckInputs = [
    pkgs.versionCheckHook
  ];
  doInstallCheck = true;

  meta = {
    description = "Extremely fast Python linter and code formatter";
    homepage = "https://github.com/astral-sh/ruff";
    changelog = "https://github.com/astral-sh/ruff/releases/tag/${version}";
    license = pkgs.lib.licenses.mit;
    mainProgram = "ruff";
    platforms = builtins.attrNames releases;
    sourceProvenance = [ pkgs.lib.sourceTypes.binaryNativeCode ];
  };
}
