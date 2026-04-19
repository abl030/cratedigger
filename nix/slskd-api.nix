{ python3Packages, fetchPypi }:

python3Packages.buildPythonPackage rec {
  pname = "slskd-api";
  version = "0.2.3";
  format = "wheel";
  src = fetchPypi {
    pname = "slskd_api";
    inherit version;
    format = "wheel";
    dist = "py3";
    python = "py3";
    hash = "sha256-X80Ct2oxzuMj8vTV9zfN0FABhekDAoejvaSs0A1pv8E=";
  };
  propagatedBuildInputs = [ python3Packages.requests ];
}
