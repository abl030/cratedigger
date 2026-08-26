# NixOS VM test for the upstream cratedigger module — the STRANGER-BOOT
# gate (tier-2 plan U10, R12): a competent NixOS stranger's first boot,
# every `nix flake check`.
#
# Posture: pipelineDb.createLocally = true (module-provisioned postgres,
# peer auth, no hand-rolled DB block), beets.validation ON, externally owned
# immutable Beets package/config plus mutable catalog/root/state, NO mirror
# knobs (public-MB defaults), and explicit operator access to a token-only
# secret include.
#
# Verifies: migrate green behind module-owned postgres ordering; immutable
# runtime config (api keys as *File paths, exact external Beets authorities,
# api_base defaults, socket DSN with no credentials); deployment-owned
# readiness; observer/importer state access; intrinsic Beets safe/hard/warning
# startup policy; service/operator package identity; a real incremental-state
# update and exact album deletion; the web UI behind the module-owned Basic-auth
# gateway and Unix socket; and structurally sound youtube-ingest + unfindable
# units.
#
# Does NOT exercise: slskd interaction or real downloads. The Beets import is
# deliberately synthetic and local; acquisition fixtures remain in Python.
{ pkgs, system, cratediggerModule, cratediggerSrc }:

let
  # Beets is deployment authority.  The VM supplies the same Python package
  # to plain `beet`, the Cratedigger applications, their checker, and the
  # harness; the public module only consumes it.
  externalBeetsPackage = import ../beets.nix { inherit pkgs; };
  externalBeetsPython = pkgs.python3.withPackages (_: [externalBeetsPackage]);
  externalLibraryRoot = "/var/lib/cratedigger-music/Beets";
  externalLibraryDb = "/var/lib/cratedigger-beets-db/beets-library.db";
  externalLibraryDbParent = builtins.dirOf externalLibraryDb;
  externalStateFile = "/var/lib/cratedigger-beets-state/state.pickle";
  externalSecretInclude = "/run/cratedigger-test-beets/discogs.yaml";

  externalBeetsSettings = {
    library = externalLibraryDb;
    directory = externalLibraryRoot;
    statefile = externalStateFile;
    asciify_paths = true;
    include = [externalSecretInclude];
    plugins = "musicbrainz mbsync discogs fetchart embedart lyrics lastgenre scrub info missing duplicates edit fromfilename ftintitle the inline permissions";
    import = {
      copy = false;
      autotag = true;
      write = true;
      move = true;
      timid = false;
      incremental = true;
      incremental_skip_later = true;
      log = "/var/lib/cratedigger-beets-db/beets-import.log";
      languages = ["en"];
      duplicate_keys = {
        album = ["mb_albumid" "discogs_albumid"];
        item = ["artist" "title"];
      };
    };
    paths = {
      default = "$albumartist/$year - $album%aunique{albumartist album,path_disambig}/$track $title";
      singleton = "Non-Album/$artist/$title";
      comp = "Compilations/$album%aunique{albumartist album,path_disambig}/$track $title";
    };
    album_fields.path_disambig =
      "albumdisambig or releasegroupdisambig or catalognum or label or str(year)";
    musicbrainz = {
      host = "musicbrainz.org";
      https = true;
      ratelimit = 1;
    };
    permissions = {
      file = "0664";
      dir = "02775";
    };
    convert = {
      auto = false;
      auto_keep = false;
    };
    fetchart.auto = true;
  };
  mkExternalBeetsConfig = name: settings: let
    yaml = (pkgs.formats.yaml {}).generate "${name}.yaml" settings;
  in pkgs.runCommand name {} ''
    mkdir -p "$out"
    ln -s ${yaml} "$out/config.yaml"
  '';
  externalBeetsConfigDir = mkExternalBeetsConfig
    "cratedigger-test-external-beets-config"
    externalBeetsSettings;
  hardBeetsConfigDir = mkExternalBeetsConfig
    "cratedigger-test-external-beets-config-hard"
    (pkgs.lib.recursiveUpdate externalBeetsSettings {
      import.write = false;
    });
  warningBeetsConfigDir = mkExternalBeetsConfig
    "cratedigger-test-external-beets-config-warning"
    (pkgs.lib.recursiveUpdate externalBeetsSettings {
      musicbrainz.host = "musicbrainz-warning.invalid";
    });
  # Generate a throwaway VM-only TLS pair in the build fixture. The private
  # key remains outside tracked source while certificateFiles installs the
  # generated public certificate into the guest trust store, so every HTTPS
  # probe still exercises certificate validation.
  publicTlsFixture = pkgs.runCommand "cratedigger-module-vm-tls" {} ''
    mkdir -p "$out"
    ${pkgs.openssl}/bin/openssl req \
      -x509 \
      -newkey ec \
      -pkeyopt ec_paramgen_curve:P-256 \
      -nodes \
      -sha256 \
      -days 36500 \
      -subj /CN=music.vm.test \
      -addext subjectAltName=DNS:music.vm.test \
      -addext basicConstraints=critical,CA:TRUE \
      -keyout "$out/private-key.pem" \
      -out "$out/certificate.pem"
  '';
  publicTlsCertificate = "${publicTlsFixture}/certificate.pem";
  publicTlsPrivateKey = "${publicTlsFixture}/private-key.pem";
  beetsDestructiveFixture = pkgs.writeText "beets-destructive-fixture.py" ''
    import os
    import sys
    from pathlib import Path
    from beets import library

    root = Path("/var/lib/cratedigger-music/Beets")
    db_path = Path("/var/lib/cratedigger-beets-db/beets-library.db")
    target_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    child_target_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    sibling_id = "dddddddd-dddd-dddd-dddd-dddddddddddd"
    target_dir = root / "Target" / "Album"
    child_target_dir = root / "Child Target" / "Album"
    sibling_dir = root / "Sibling" / "Album"
    sibling_path = sibling_dir / "01 Sibling.flac"

    if sys.argv[1] == "seed":
        os.umask(0o002)
        target_dir.mkdir(parents=True)
        child_target_dir.mkdir(parents=True)
        sibling_dir.mkdir(parents=True)
        items = []
        for index in range(1, 13):
            path = target_dir / f"{index:02d} Track.flac"
            path.write_bytes(f"audio-{index}".encode())
            items.append(library.Item(
                path=str(path), title=f"Track {index}", artist="Target",
                album="Album", albumartist="Target", mb_albumid=target_id,
            ))
        child_items = []
        for index in range(1, 13):
            path = child_target_dir / f"{index:02d} Track.flac"
            path.write_bytes(f"child-audio-{index}".encode())
            child_items.append(library.Item(
                path=str(path), title=f"Track {index}", artist="Child Target",
                album="Album", albumartist="Child Target",
                mb_albumid=child_target_id,
            ))
        sibling_path.write_bytes(b"rare sibling")
        lib = library.Library(str(db_path), str(root))
        lib.add_album(items)
        child_album = lib.add_album(child_items)
        lib.add_album([library.Item(
            path=str(sibling_path), title="Sibling", artist="Sibling",
            album="Album", albumartist="Sibling", mb_albumid=sibling_id,
        )])
        lib._close()
        db_path.chmod(0o664)
        print(f"CHILD_ALBUM_ID={child_album.id}")
    elif sys.argv[1] == "verify":
        lib = library.Library(str(db_path), str(root))
        assert not list(lib.albums(f"mb_albumid:{target_id}"))
        assert not list(lib.albums(f"mb_albumid:{child_target_id}"))
        sibling = list(lib.albums(f"mb_albumid:{sibling_id}"))
        assert len(sibling) == 1, sibling
        assert len(list(sibling[0].items())) == 1
        lib._close()
        assert not target_dir.exists(), target_dir
        assert not child_target_dir.exists(), child_target_dir
        assert sibling_path.read_bytes() == b"rare sibling"
    else:
        raise AssertionError(sys.argv)
  '';
  headerRecorder = pkgs.writeText "cratedigger-test-header-recorder.py" ''
    import http.server
    import json
    import os
    import socket
    import socketserver

    RECORD_PATH = "/var/lib/cratedigger/test-header-recorder.jsonl"


    class RecorderHandler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _record(self):
            raw_items = list(self.headers.raw_items())
            names = sorted({name.lower() for name, _ in raw_items})
            headers = {
                name: self.headers.get_all(name, failobj=[])
                for name in names
            }
            raw_length = self.headers.get("Content-Length")
            # Deliberately reproduce the old health-handler shape: leave any
            # declared body unread so a missing nginx bodyless/close boundary
            # would parse it as a second request.
            if raw_length is not None and self.path != "/healthz":
                self.rfile.read(int(raw_length))
            row = {
                "method": self.command,
                "path": self.path,
                "headers": headers,
                "raw_headers": raw_items,
            }
            fd = os.open(
                RECORD_PATH,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            with os.fdopen(fd, "a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(row, separators=(",", ":"), sort_keys=True)
                    + "\n"
                )
            self.send_response(204 if self.path == "/healthz" else 200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_GET = _record
        do_HEAD = _record
        do_POST = _record

        def log_message(self, _format, *_args):
            pass


    class RecorderServer(
        socketserver.ThreadingMixIn,
        socketserver.UnixStreamServer,
    ):
        daemon_threads = True

        def __init__(self, listener):
            socketserver.BaseServer.__init__(
                self,
                listener.getsockname(),
                RecorderHandler,
            )
            self.socket = listener
            self.server_name = "cratedigger.internal"
            self.server_port = 0


    listener = socket.socket(fileno=3)
    listener.set_inheritable(False)
    RecorderServer(listener).serve_forever()
  '';
  rawHttpProbe = pkgs.writeText "cratedigger-test-raw-http.py" ''
    import base64
    import json
    import socket
    import ssl
    import sys

    request = base64.b64decode(sys.stdin.buffer.read(), validate=True)
    target = sys.argv[1]
    if target == "gateway":
        port = 18086
    elif target == "public":
        port = 18443
    else:
        raise AssertionError(target)
    response = bytearray()
    reached_eof = False
    timed_out = False
    transport = socket.create_connection(("127.0.0.1", port), timeout=2.0)
    if target == "public":
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        client = context.wrap_socket(transport, server_hostname="music.vm.test")
    else:
        client = transport
    with client:
        client.settimeout(2.0)
        client.sendall(request)
        while True:
            try:
                chunk = client.recv(65536)
            except TimeoutError:
                timed_out = True
                break
            if not chunk:
                reached_eof = True
                break
            response.extend(chunk)
    print(json.dumps({
        "response": base64.b64encode(response).decode("ascii"),
        "reached_eof": reached_eof,
        "timed_out": timed_out,
    }, separators=(",", ":"), sort_keys=True))
  '';
  productionWebConcurrencyProbe =
    pkgs.writeText "cratedigger-test-production-web-concurrency.py" ''
      import os
      import socket
      import time

      BLOCKED_PATH = "/tmp/cratedigger-production-web-concurrency.blocked"
      RELEASE_PATH = "/tmp/cratedigger-production-web-concurrency.release"
      SOCKET_PATH = "/run/cratedigger-web/web.sock"

      response = bytearray()
      with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
          client.settimeout(12)
          client.connect(SOCKET_PATH)
          client.sendall(
              b"GET /api/_index HTTP/1.1\r\n"
              b"Host: music.vm.test\r\n"
              b"X-Cratedigger-Request-Channel: browser\r\n"
              b"X-Concurrency-Probe: held"
          )
          marker_fd = os.open(
              BLOCKED_PATH,
              os.O_WRONLY | os.O_CREAT | os.O_EXCL,
              0o600,
          )
          with os.fdopen(marker_fd, "w", encoding="utf-8") as marker:
              marker.write("blocked\n")

          deadline = time.monotonic() + 10
          while not os.path.exists(RELEASE_PATH):
              if time.monotonic() >= deadline:
                  raise TimeoutError("production web release marker timed out")
              time.sleep(0.01)

          client.sendall(b"\r\nConnection: close\r\n\r\n")
          while True:
              chunk = client.recv(65536)
              if not chunk:
                  break
              response.extend(chunk)

      status_line = bytes(response).split(b"\r\n", 1)[0]
      status_parts = status_line.split()
      if len(status_parts) < 2 or status_parts[1] != b"200":
          raise AssertionError(
              f"production web probe returned {status_line!r}"
          )
    '';
in
pkgs.testers.nixosTest {
  name = "cratedigger-module-vm";

  nodes.machine = { config, lib, pkgs, ... }: let
    configHoldGate = pkgs.writeShellScript "cratedigger-test-config-hold" ''
      test ! -e /run/cratedigger-test-config-hold
    '';
    heldApplicationServiceNames = [
      "cratedigger"
      "cratedigger-unfindable"
      "cratedigger-importer"
      "cratedigger-import-preview-worker"
      "cratedigger-youtube-ingest"
      "cratedigger-web"
    ];
    metadataGateServiceNames = [
      "cratedigger"
      "cratedigger-importer"
      "cratedigger-import-preview-worker"
      "cratedigger-youtube-ingest"
      "cratedigger-web"
    ];
    heldApplicationUnits = map (name: "${name}.service")
      heldApplicationServiceNames;
    beetsReadinessFixture = pkgs.writeShellScript
      "cratedigger-test-beets-readiness" ''
        set -euo pipefail
        if test -e /run/cratedigger-test-beets-readiness-fail; then
          echo BEETS_EXTERNAL_READINESS_FAILED >&2
          exit 1
        fi
        ${pkgs.coreutils}/bin/install -d \
          -o root -g beets-library -m 2775 \
          ${externalLibraryRoot} \
          $(${pkgs.coreutils}/bin/dirname ${externalLibraryDb})
        ${pkgs.coreutils}/bin/install -d \
          -o root -g root -m 0755 \
          $(${pkgs.coreutils}/bin/dirname ${externalStateFile})
        ${pkgs.coreutils}/bin/install -d \
          -o root -g beets-library -m 0750 \
          $(${pkgs.coreutils}/bin/dirname ${externalSecretInclude})

        ${pkgs.coreutils}/bin/head -c 24 /dev/urandom \
          | ${pkgs.coreutils}/bin/base64 \
          | ${pkgs.coreutils}/bin/tr -d '\n' \
          > /run/cratedigger-test-beets/token
        token="$(${pkgs.coreutils}/bin/cat /run/cratedigger-test-beets/token)"
        ${pkgs.coreutils}/bin/printf \
          'discogs:\n  user_token: "%s"\n' "$token" \
          > ${externalSecretInclude}
        ${pkgs.coreutils}/bin/rm -f /run/cratedigger-test-beets/token
        ${pkgs.coreutils}/bin/chown root:beets-library ${externalSecretInclude}
        ${pkgs.coreutils}/bin/chmod 0440 ${externalSecretInclude}

        ${externalBeetsPython}/bin/python - <<'PY'
        import pickle
        from pathlib import Path

        path = Path("${externalStateFile}")
        path.write_bytes(pickle.dumps({}))
        PY
        ${pkgs.coreutils}/bin/chown root:beets-library ${externalStateFile}
        ${pkgs.coreutils}/bin/chmod 0660 ${externalStateFile}

        BEETSDIR=${externalBeetsConfigDir} \
          ${externalBeetsPython}/bin/python - <<'PY'
        from beets.library import Library

        library = Library("${externalLibraryDb}", "${externalLibraryRoot}")
        library._close()
        PY
        ${pkgs.coreutils}/bin/chown \
          cratedigger:beets-library ${externalLibraryDb}
        ${pkgs.coreutils}/bin/chmod 0664 ${externalLibraryDb}
        ${pkgs.coreutils}/bin/echo BEETS_EXTERNAL_READINESS_OK
      '';
    mkBeetsAccessProbe = name: stateWritable:
      pkgs.writeShellScript "cratedigger-test-beets-access-${name}" ''
        set -euo pipefail
        if ${pkgs.python3}/bin/python3 -c \
          'import os; fd=os.open("${externalBeetsConfigDir}/config.yaml", os.O_WRONLY); os.close(fd)' \
          2>/dev/null; then
          echo "${name} could write immutable BEETSDIR" >&2
          exit 1
        fi
        ${if stateWritable then ''
          ${pkgs.python3}/bin/python3 -c \
            'import os; fd=os.open("${externalStateFile}", os.O_WRONLY); os.close(fd)'
        '' else ''
          if ${pkgs.python3}/bin/python3 -c \
            'import os; fd=os.open("${externalStateFile}", os.O_WRONLY); os.close(fd)' \
            2>/dev/null; then
            echo "${name} could write importer-only Beets state" >&2
            exit 1
          fi
        ''}
      '';
    beetsObserverAccessProbe = mkBeetsAccessProbe "observer" false;
    beetsMainAccessProbe = pkgs.writeShellScript
      "cratedigger-test-beets-access-main" ''
        set -euo pipefail
        ${beetsObserverAccessProbe}
        for probe in \
          ${externalLibraryRoot}/.cratedigger-main-write-probe \
          ${externalLibraryDbParent}/.cratedigger-main-write-probe
        do
          if ${pkgs.coreutils}/bin/touch "$probe" 2>/dev/null; then
            ${pkgs.coreutils}/bin/rm -f "$probe"
            echo "main could write external Beets library authority: $probe" >&2
            exit 1
          fi
        done
        echo BEETS_MAIN_WRITE_DENIAL_OK
      '';
    beetsImporterAccessProbe = mkBeetsAccessProbe "importer" true;
    metadataGateStateDir = "/var/lib/cratedigger-metadata-gate";
    metadataGateMainStartInhibitor =
      "${metadataGateStateDir}/inhibit-cratedigger.service";
    metadataGateYoutubeStartInhibitor =
      "${metadataGateStateDir}/inhibit-cratedigger-youtube-ingest.service";
    importerSandboxProbe = pkgs.writeShellScript "cratedigger-importer-sandbox-probe" ''
      set -euo pipefail
      probe_dir=/var/lib/cratedigger/processing/sandbox-probe
      ${pkgs.coreutils}/bin/install -d -m 0700 "$probe_dir"
      test -f ${externalBeetsConfigDir}/config.yaml

      # Run representative shipped media/Beets tools inside the importer's
      # actual service sandbox and @system-service syscall filter.
      ${pkgs.sox}/bin/sox \
        -n -r 44100 -c 1 "$probe_dir/tone.wav" synth 0.05 sine 440
      ${pkgs.ffmpeg}/bin/ffmpeg \
        -nostdin -loglevel error -y -i "$probe_dir/tone.wav" \
        -codec:a libmp3lame "$probe_dir/tone.mp3"
      ${pkgs.mp3val}/bin/mp3val "$probe_dir/tone.mp3" >/dev/null
      BEETSDIR=${externalBeetsConfigDir} \
        ${externalBeetsPackage}/bin/beet version >/dev/null

      # Exercise Beets' real incremental importer exactly once inside the
      # importer's actual writable-state namespace.  It must update the
      # external state file without changing immutable BEETSDIR.
      incremental_receipt=/var/lib/cratedigger/beets-incremental-vm.receipt
      if test ! -e "$incremental_receipt"; then
        ${pkgs.coreutils}/bin/cp \
          "$probe_dir/tone.mp3" "$probe_dir/incremental.mp3"
        state_before="$(${pkgs.coreutils}/bin/sha256sum \
          ${externalStateFile} | ${pkgs.coreutils}/bin/cut -d ' ' -f 1)"
        config_before="$(${pkgs.coreutils}/bin/sha256sum \
          ${externalBeetsConfigDir}/config.yaml \
          | ${pkgs.coreutils}/bin/cut -d ' ' -f 1)"
        BEETSDIR=${externalBeetsConfigDir} \
          ${externalBeetsPackage}/bin/beet import -Aq -s \
          "$probe_dir/incremental.mp3"
        state_after="$(${pkgs.coreutils}/bin/sha256sum \
          ${externalStateFile} | ${pkgs.coreutils}/bin/cut -d ' ' -f 1)"
        config_after="$(${pkgs.coreutils}/bin/sha256sum \
          ${externalBeetsConfigDir}/config.yaml \
          | ${pkgs.coreutils}/bin/cut -d ' ' -f 1)"
        test "$state_before" != "$state_after"
        test "$config_before" = "$config_after"
        ${pkgs.coreutils}/bin/printf '%s %s %s\n' \
          "$state_before" "$state_after" "$config_after" \
          > "$incremental_receipt"
      fi

      # Each importer authority root must remain writable inside the mount
      # namespace. A world-writable directory outside ReadWritePaths must not.
      touch /var/lib/cratedigger/.sandbox-probe
      touch /var/lib/cratedigger/processing/.sandbox-probe
      touch /var/lib/cratedigger-downloads/.sandbox-probe
      touch /var/lib/cratedigger-music/Beets/.sandbox-probe
      touch /var/lib/cratedigger-beets-db/.sandbox-probe
      touch /var/lib/cratedigger-music/Incoming/.sandbox-probe
      touch /var/lib/cratedigger-music/Re-download/.sandbox-probe
      if touch /var/lib/cratedigger-music/unrelated/escape 2>/dev/null; then
        echo "sandbox allowed a write to an unrelated music sibling" >&2
        exit 1
      fi
      if touch /var/lib/cratedigger-world-writable/escape 2>/dev/null; then
        echo "sandbox allowed a write outside ReadWritePaths" >&2
        exit 1
      fi
    '';
    stateDbDenialProbe = pkgs.writeShellScript "cratedigger-state-db-denial-probe" ''
      set -euo pipefail
      probe=/var/lib/cratedigger-beets-db/worker-escape
      if touch "$probe" 2>/dev/null; then
        rm -f "$probe"
        echo "sandbox allowed a worker write to the Beets DB parent" >&2
        exit 1
      fi
    '';
    deployHoldPipelineCli = pkgs.writeShellScriptBin "pipeline-cli" ''
      exec ${pkgs.util-linux}/bin/runuser -u cratedigger -- \
        /run/current-system/sw/bin/pipeline-cli "$@"
    '';
    deployHoldTool = pkgs.writeShellScriptBin "cratedigger-deploy-hold" ''
      export PATH="${deployHoldPipelineCli}/bin:$PATH"
      exec ${pkgs.python3}/bin/python3 \
        ${cratediggerSrc}/scripts/cratedigger_deploy_hold.py "$@"
    '';
    metadataGateTool = pkgs.writeShellScriptBin "cratedigger-metadata-gate" ''
      set -euo pipefail
      state_dir=${metadataGateStateDir}
      hold_dir="$state_dir/holds"
      guarded_units=(cratedigger.timer cratedigger.service cratedigger-web.service cratedigger-importer.service cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service)
      resume_units=(cratedigger.service cratedigger.timer cratedigger-web.service cratedigger-importer.service cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service)

      ${pkgs.coreutils}/bin/install \
        -d -o root -g root -m 0755 "$state_dir" "$hold_dir"
      case "''${1:-}" in
        hold)
          test "''${2:-}" = manual
          printf 'manual\n' > "$hold_dir/manual"
          ;;
        release)
          test "''${2:-}" = manual
          ${pkgs.coreutils}/bin/rm -f "$hold_dir/manual"
          ;;
        start-check|resume-if-clear)
          test -z "$(
            ${pkgs.findutils}/bin/find \
              "$hold_dir" -mindepth 1 -maxdepth 1 -print -quit
          )"
          ;;
        *) exit 64 ;;
      esac
    '';
    metadataGateStartCheck =
      "+${metadataGateTool}/bin/cratedigger-metadata-gate start-check";
    deployHoldBlocker = pkgs.writeShellScript "cratedigger-deploy-hold-blocker" ''
      set -euo pipefail
      while test -e /run/cratedigger-deploy-hold-blocker; do
        sleep 0.1
      done
    '';
    basicAuthFixture = pkgs.writeShellScript "cratedigger-test-basic-auth" ''
      set -euo pipefail
      ${pkgs.coreutils}/bin/install \
        -d -o root -g ${config.services.nginx.group} -m 0750 \
        /run/cratedigger-test-auth
      ${pkgs.apacheHttpd}/bin/htpasswd \
        -bcB -C 4 \
        /run/cratedigger-test-auth/basic.htpasswd \
        test-operator test-password
      ${pkgs.coreutils}/bin/chown \
        root:${config.services.nginx.group} \
        /run/cratedigger-test-auth/basic.htpasswd
      ${pkgs.coreutils}/bin/chmod \
        0440 /run/cratedigger-test-auth/basic.htpasswd
      ${pkgs.coreutils}/bin/install \
        -o root \
        -g ${config.services.nginx.group} \
        -m 0440 \
        /run/cratedigger-test-auth/basic.htpasswd \
        /run/cratedigger-test-auth/basic-alternate.htpasswd
    '';
    policyMutationDuringReload = pkgs.writeShellScript
      "cratedigger-test-policy-mutation-during-reload"
      ''
        set -euo pipefail

        trigger=/run/cratedigger-test-mutate-policy-during-reload
        ${pkgs.coreutils}/bin/test -e "$trigger" || exit 0
        ${pkgs.coreutils}/bin/rm -f -- "$trigger"
        ${pkgs.coreutils}/bin/test \
          "$(${pkgs.coreutils}/bin/stat -c '%U:%G:%a' \
            /run/cratedigger-web/gateway-reload-receipt)" \
          = root:root:600
        ${pkgs.coreutils}/bin/install \
          -o root -g root -m 0600 \
          /etc/cratedigger/web-gateway-policy \
          /run/cratedigger-test-policy-original
        original_sha="$(
          ${pkgs.coreutils}/bin/sha256sum \
            /etc/cratedigger/web-gateway-policy \
            | ${pkgs.coreutils}/bin/cut -d ' ' -f 1
        )"
        byte_count="$(
          ${pkgs.coreutils}/bin/stat -c %s \
            /etc/cratedigger/web-gateway-policy
        )"
        ${pkgs.coreutils}/bin/test "$byte_count" -gt 0
        ${pkgs.coreutils}/bin/test \
          "$(
            ${pkgs.coreutils}/bin/tail -c 1 \
              /etc/cratedigger/web-gateway-policy \
              | ${pkgs.coreutils}/bin/od -An -tx1 \
              | ${pkgs.coreutils}/bin/tr -d '[:space:]'
          )" = 0a
        mutated="$(${pkgs.coreutils}/bin/mktemp)"
        trap '${pkgs.coreutils}/bin/rm -f -- "$mutated"' EXIT
        ${pkgs.coreutils}/bin/head -c "$((byte_count - 1))" \
          /etc/cratedigger/web-gateway-policy > "$mutated"
        mutated_sha="$(
          ${pkgs.coreutils}/bin/sha256sum "$mutated" \
            | ${pkgs.coreutils}/bin/cut -d ' ' -f 1
        )"
        ${pkgs.coreutils}/bin/test "$mutated_sha" != "$original_sha"
        ${pkgs.coreutils}/bin/test \
          "$(${pkgs.coreutils}/bin/wc -l < "$mutated")" = 3
        ${pkgs.coreutils}/bin/rm -f \
          /etc/cratedigger/web-gateway-policy
        ${pkgs.coreutils}/bin/install \
          -o root -g root -m 0444 \
          "$mutated" /etc/cratedigger/web-gateway-policy
      '';
    credentialMutationDuringReload = pkgs.writeShellScript
      "cratedigger-test-credential-mutation-during-reload"
      ''
        set -euo pipefail

        trigger=/run/cratedigger-test-mutate-credential-during-reload
        ${pkgs.coreutils}/bin/test -e "$trigger" || exit 0
        ${pkgs.coreutils}/bin/rm -f -- "$trigger"
        ${pkgs.coreutils}/bin/test \
          "$(${pkgs.coreutils}/bin/stat -c '%U:%G:%a' \
            /run/cratedigger-web/gateway-reload-receipt)" \
          = root:root:600
        replacement="$(
          ${pkgs.coreutils}/bin/mktemp \
            /run/cratedigger-test-auth/basic.htpasswd.swap.XXXXXX
        )"
        trap '${pkgs.coreutils}/bin/rm -f -- "$replacement"' EXIT
        ${pkgs.coreutils}/bin/install \
          -o root -g ${config.services.nginx.group} -m 0440 \
          /run/cratedigger-test-auth/basic-rotated.htpasswd \
          "$replacement"
        ${pkgs.coreutils}/bin/mv -T \
          "$replacement" \
          /run/cratedigger-test-auth/basic.htpasswd
        trap - EXIT
      '';
  in {
    imports = [ cratediggerModule ];

    # Fake slskd API key — never actually called because healthCheck is off.
    environment.etc."cratedigger/slskd-api-key" = {
      text = "test-api-key-do-not-use\n";
      mode = "0400";
      user = "cratedigger";
      group = "beets-library";
    };
    users.users.beets-operator = {
      isNormalUser = true;
      extraGroups = ["beets-library" "cratedigger-web"];
    };
    users.users.unrelated-user.isNormalUser = true;
    users.groups.beets-library = {};
    users.groups.slskd-writer = {};
    users.users.slskd-writer = {
      isSystemUser = true;
      group = "slskd-writer";
    };
    # The source-owner group is separate from the private processor group.
    # The service can consume event-stamped source bytes but never grants
    # the writer any authority over its processing root.
    # Exercise the module's actual default cratedigger:cratedigger identity;
    # deployment-owned groups provide only the external authorities it needs.
    users.users.cratedigger.extraGroups = [ "beets-library" "slskd-writer" ];
    networking.hosts."127.0.0.1" = [
      "music.vm.test"
      "unrelated.vm.test"
    ];
    security.pki.certificateFiles = [publicTlsCertificate];

    services.cratedigger = {
      enable = true;
      src = cratediggerSrc;
      slskd = {
        apiKeyFile = "/etc/cratedigger/slskd-api-key";
        hostUrl = "http://192.0.2.21:5030";
        downloadDir = "/var/lib/cratedigger-downloads";
      };
      # Stranger posture (U7/R10): the module provisions PostgreSQL —
      # role + database named after the non-root cfg.user, unix-socket
      # peer auth, DSN defaulted to the socket. No hand-rolled postgres
      # block, no manual unit ordering, no password material anywhere.
      pipelineDb.createLocally = true;
      beets.validation = {
        enable = true;
        stagingDir = "/var/lib/cratedigger-music/Incoming";
        # Keep the tracking parent distinct from staging so the sandbox
        # contract proves it is derived from its own option.
        trackingFile = "/var/lib/cratedigger-music/Re-download/tracking.jsonl";
      };
      beets.runtime = {
        package = externalBeetsPackage;
        configDir = toString externalBeetsConfigDir;
        expectedLibrary = externalLibraryDb;
        expectedDirectory = externalLibraryRoot;
        expectedStateFile = externalStateFile;
        expectedSecretInclude = externalSecretInclude;
        readinessUnits = ["cratedigger-test-beets-readiness.service"];
      };
      web = {
        enable = true;
        hostName = "music.vm.test";
        gatewayPort = 18086;
        gatewayAddresses = ["127.0.0.1" "127.0.0.2" "[::1]"];
        basicAuthFile = "/run/cratedigger-test-auth/basic.htpasswd";
      };
      # Enable the YouTube-rescue ingest worker so its unit is rendered.
      # We only assert structural properties (dependencies, PATH, lock
      # contention) — the worker process itself starts but stays idle
      # because no download_log source='youtube' outcome='youtube_running'
      # rows exist in the test DB.
      youtubeIngest.enable = true;
      # Host-specific VPN-NIC bind address (KTD9). The VM's --once run
      # never invokes yt-dlp (empty queue), so this is exercised only at
      # the wrapper-render seam: we assert the flag lands in the ExecStart.
      youtubeIngest.sourceAddress = "10.0.2.15";
      # Render the real NixOS-managed timer while keeping it far from firing.
      # The deploy-hold VM regression below needs the actual /etc unit path.
      timer = {
        enable = true;
        onBootSec = "1d";
        onUnitInactiveSec = "1d";
      };
      healthCheck.enable = false;
    };

    # Mirror the downstream localProxy topology: a public TLS listener owns
    # the canonical hostname and forwards only to the module's loopback
    # gateway with the public scheme/port envelope. A separate default vhost
    # rejects non-canonical Host values before they can be canonicalised.
    services.nginx.virtualHosts = {
      cratedigger-test-public = {
        serverName = "music.vm.test";
        onlySSL = true;
        listen = [
          {
            addr = "0.0.0.0";
            port = 18443;
            ssl = true;
          }
          {
            addr = "[::]";
            port = 18443;
            ssl = true;
          }
        ];
        sslCertificate = publicTlsCertificate;
        sslCertificateKey = publicTlsPrivateKey;
        locations."/".extraConfig = ''
          proxy_http_version 1.1;
          proxy_pass http://127.0.0.1:18086;
          proxy_set_header Host music.vm.test;
          proxy_set_header X-Forwarded-Proto https;
          proxy_set_header X-Forwarded-Port 443;
          proxy_set_header Connection "";
        '';
      };
      # A downstream-added application location inherits server-scope Basic.
      # It intentionally declares no location-level auth setting.
      cratedigger-auth-gateway.locations."/inherited-basic" = {
        proxyPass = "http://unix:/run/cratedigger-web/web.sock:";
        recommendedProxySettings = false;
      };
      cratedigger-test-public-reject = {
        default = true;
        serverName = "_";
        onlySSL = true;
        listen = [
          {
            addr = "0.0.0.0";
            port = 18443;
            ssl = true;
          }
          {
            addr = "[::]";
            port = 18443;
            ssl = true;
          }
        ];
        sslCertificate = publicTlsCertificate;
        sslCertificateKey = publicTlsPrivateKey;
        locations."/".extraConfig = "return 444;";
      };
      cratedigger-test-unrelated = {
        serverName = "unrelated.vm.test";
        listen = [
          {
            addr = "127.0.0.1";
            port = 18087;
          }
        ];
        locations."/".extraConfig = "return 204;";
      };
    };

    # A real activation target for the authentication-mode lifecycle below.
    # The ordinary system remains Basic; the specialisation changes only the
    # explicitly selected mode so switch-to-configuration exercises the same
    # nginx and web-service transition as a deployment.
    specialisation.cratedigger-insecure.configuration = {
      services.cratedigger.web = {
        basicAuthFile = lib.mkForce null;
        enableInsecure = lib.mkForce true;
      };
    };
    # Issue #1161. A real activation target whose ONLY difference is the
    # migrate unit's own rendered unit file, so switch-to-configuration
    # classifies exactly that unit as changed and we can observe which list it
    # routes it into. restartTriggers is the least invasive way to change the
    # unit file without touching its semantics.
    specialisation.cratedigger-migrate-bump.configuration = {
      systemd.services.cratedigger-db-migrate.restartTriggers = [
        "issue-1161-routing-probe"
      ];
    };
    specialisation.cratedigger-basic-alternate.configuration = {
      services.cratedigger.web = {
        basicAuthFile = lib.mkForce
          "/run/cratedigger-test-auth/basic-alternate.htpasswd";
        enableInsecure = lib.mkForce false;
      };
    };
    # External authorization as a downstream operator actually deploys it: the
    # front proxy authorizes every request before forwarding, and Cratedigger
    # is not a participant. The stub authorizer below is a forward-auth
    # endpoint reduced to the one decision the module's contract depends on —
    # allow or deny — which is the same shape Authelia, authentik,
    # oauth2-proxy, and Pocket ID present to nginx.
    #
    # It also injects the identity headers a real authorizer would set, so the
    # composed path proves the module gateway drops them rather than proving
    # it in isolation.
    specialisation.cratedigger-external-auth.configuration = {
      services.cratedigger.web = {
        basicAuthFile = lib.mkForce null;
        enableInsecure = lib.mkForce false;
        externalAuth = lib.mkForce true;
      };
      services.nginx.virtualHosts.cratedigger-test-public = {
        locations."= /stub-authz".extraConfig = ''
          internal;
          if ($http_cookie ~ "vm_session=granted") {
            return 204;
          }
          return 401;
        '';
        locations."/".extraConfig = lib.mkForce ''
          auth_request /stub-authz;
          proxy_http_version 1.1;
          proxy_pass http://127.0.0.1:18086;
          proxy_set_header Host music.vm.test;
          proxy_set_header X-Forwarded-Proto https;
          proxy_set_header X-Forwarded-Port 443;
          proxy_set_header Connection "";
          proxy_set_header Remote-User "vm-operator";
          proxy_set_header Remote-Groups "vm-admins";
          proxy_set_header Remote-Email "vm-operator@example.test";
        '';
      };
    };
    specialisation.cratedigger-beets-hard.configuration = {
      services.cratedigger.beets.runtime.configDir =
        lib.mkForce (toString hardBeetsConfigDir);
      systemd.services = lib.genAttrs heldApplicationServiceNames (name:
        {
          wantedBy = lib.mkForce [];
          restartIfChanged = lib.mkForce false;
        } // lib.optionalAttrs (builtins.elem name [
          "cratedigger-importer"
          "cratedigger-import-preview-worker"
          "cratedigger-web"
        ]) {
          serviceConfig.Restart = lib.mkForce "no";
        });
    };
    specialisation.cratedigger-beets-warning.configuration = {
      services.cratedigger.beets.runtime.configDir =
        lib.mkForce (toString warningBeetsConfigDir);
      systemd.services = lib.genAttrs heldApplicationServiceNames (_: {
        restartIfChanged = lib.mkForce false;
      });
    };

    environment.systemPackages = [
      deployHoldTool
      metadataGateTool
      externalBeetsPackage
    ];

    # Simulate a downstream metadata gate holding every application unit. The
    # immutable runtime configuration already exists in the store; the test
    # removes this hold before exercising the apps.
    systemd.tmpfiles.rules = [
      "d /var/lib/cratedigger-music 0777 root root -"
      "d /var/lib/cratedigger-music/Incoming 2775 cratedigger beets-library -"
      "d /var/lib/cratedigger-music/Re-download 0755 cratedigger beets-library -"
      "d /var/lib/cratedigger-music/unrelated 0777 root root -"
      "d /var/lib/cratedigger-downloads 0770 slskd-writer slskd-writer -"
      "d /var/lib/cratedigger-world-writable 0777 root root -"
    ];
    systemd.services = lib.mkMerge [
      (lib.genAttrs heldApplicationServiceNames (_: {
        after = ["cratedigger-test-config-hold.service"];
        requires = ["cratedigger-test-config-hold.service"];
        serviceConfig.ExecCondition = lib.mkAfter [configHoldGate];
      }))
      (lib.genAttrs metadataGateServiceNames (_: {
        serviceConfig.ExecCondition = lib.mkBefore [metadataGateStartCheck];
      }))
      {
        cratedigger.unitConfig.ConditionPathExists =
          "!${metadataGateMainStartInhibitor}";
        cratedigger-youtube-ingest.unitConfig.ConditionPathExists =
          "!${metadataGateYoutubeStartInhibitor}";

        # Create the downstream-gate fixture once at boot. Keep this oneshot
        # active and unchanged across specialisation switches so activation
        # cannot recreate the marker after the test deliberately removes it.
        cratedigger-test-config-hold = {
          description = "Hold Cratedigger application units on first boot";
          wantedBy = ["multi-user.target"];
          before = heldApplicationUnits;
          restartIfChanged = false;
          script = ''
            ${pkgs.coreutils}/bin/install -m 0644 /dev/null \
              /run/cratedigger-test-config-hold
            ${pkgs.coreutils}/bin/printf 'held\n' \
              > /run/cratedigger-test-config-hold
          '';
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
          };
        };
        cratedigger-test-beets-readiness = {
          description = "Provision external Beets authority for the VM";
          wantedBy = ["multi-user.target"];
          before = heldApplicationUnits;
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = beetsReadinessFixture;
          };
        };
        # Test-only runtime secret provisioning. The bcrypt hash is generated
        # in the VM, so neither the active file nor its resolved target is a
        # Nix-store path and the hash cannot be embedded in generated config.
        cratedigger-test-basic-auth = {
          description = "Provision the Cratedigger module VM Basic credential";
          before = ["nginx.service"];
          serviceConfig = {
            Type = "oneshot";
            RemainAfterExit = true;
            ExecStart = basicAuthFixture;
          };
        };
        nginx = {
          after = ["cratedigger-test-basic-auth.service"];
          requires = ["cratedigger-test-basic-auth.service"];
          # Run after nginx's ordinary config-test/HUP commands (priority
          # 1000), but before Cratedigger's receipt-bound finish hook
          # (mkAfter, priority 1500). The trigger is absent outside the one
          # deterministic overlap test.
          serviceConfig.ExecReload = lib.mkOrder 1400 [
            "+${policyMutationDuringReload}"
            "+${credentialMutationDuringReload}"
          ];
        };
        cratedigger.serviceConfig.ExecStartPre =
          lib.mkAfter [beetsMainAccessProbe];
        cratedigger-importer.serviceConfig.ExecStartPre =
          lib.mkAfter [beetsImporterAccessProbe importerSandboxProbe];
        # Preview and YouTube retain stateDir for their established workflows,
        # but the sibling DB parent must remain unreachable in both sandboxes.
        cratedigger-import-preview-worker.serviceConfig.ExecStartPre =
          lib.mkAfter [beetsObserverAccessProbe stateDbDenialProbe];
        cratedigger-youtube-ingest.serviceConfig.ExecStartPre =
          lib.mkAfter [stateDbDenialProbe];
        cratedigger-web.serviceConfig.ExecStartPre =
          lib.mkAfter [beetsObserverAccessProbe];
        # The blocker has no dependency edge from the application units. Its
        # ordering matters only while the VM test has explicitly queued both
        # jobs, which gives us a deterministic real systemd `start/waiting`.
        cratedigger.after = ["cratedigger-deploy-hold-blocker.service"];
        cratedigger-unfindable.after = ["cratedigger-deploy-hold-blocker.service"];
        cratedigger-metadata-gate-watchdog = {
          after = ["cratedigger-deploy-hold-blocker.service"];
          serviceConfig = {
            Type = "oneshot";
            ExecStart = "${pkgs.coreutils}/bin/true";
          };
        };
        cratedigger-deploy-hold-blocker.serviceConfig = {
          Type = "oneshot";
          ExecStart = deployHoldBlocker;
        };
      }
    ];

    systemd.timers.cratedigger-metadata-gate-watchdog = {
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = "1d";
        OnUnitInactiveSec = "1d";
      };
    };

    # NO manual postgres ordering: the module owns
    # cratedigger-db-migrate's after/requires on postgresql-setup.service when
    # createLocally is set, and every app unit requires the migrate unit —
    # transitively serialising first boot behind role/database provisioning.

    # Python-heavy services repeatedly read their closures during this test.
    # A guest-local store image avoids making those reads cross 9p. Keep its
    # default tmpfs-backed writable overlay because the test queries closures
    # with nix-store, which maintains store bookkeeping while it runs.
    virtualisation.memorySize = 2048;
    # The guest core count has no explicit default in this file, so it was
    # silently inheriting the qemu-vm module's default of 1 (issue #1131) —
    # serializing PostgreSQL, nginx, ~10 switch-to-configuration calls, and 2
    # reboots onto one emulated core. Give it real parallelism.
    virtualisation.cores = 4;
    virtualisation.useNixStoreImage = true;
    virtualisation.writableStore = true;
  };

  # Keep the large Python program in its own store file. Nix otherwise places
  # the whole script in one builder environment entry and eventually crosses
  # Linux's per-string exec limit before the VM can start.
  testScript = let
    script = pkgs.writeText "cratedigger-module-vm-test.py" ''
    import base64
    import json
    import re
    import shlex

    def _response_header_values(raw_headers, expected_name):
        values = []
        for line in raw_headers.splitlines():
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            if name.lower() == expected_name.lower():
                values.append(value.strip())
        return values

    def _assert_exact_response_header(raw_headers, name, value):
        values = _response_header_values(raw_headers, name)
        assert values == [value], (name, value, values, raw_headers)

    def _assert_absent_response_header(raw_headers, name):
        values = _response_header_values(raw_headers, name)
        assert values == [], (name, values, raw_headers)

    def _assert_health_response_headers(raw_headers):
        _assert_exact_response_header(
            raw_headers,
            "Content-Security-Policy",
            "frame-ancestors 'none'",
        )
        _assert_exact_response_header(
            raw_headers,
            "X-Frame-Options",
            "DENY",
        )
        _assert_exact_response_header(
            raw_headers,
            "Cross-Origin-Resource-Policy",
            "same-origin",
        )
        _assert_absent_response_header(raw_headers, "WWW-Authenticate")
        for cors_name in (
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Credentials",
            "Access-Control-Allow-Methods",
            "Access-Control-Allow-Headers",
        ):
            _assert_absent_response_header(raw_headers, cors_name)

    def _raw_http(request, target):
        encoded = base64.b64encode(request).decode("ascii")
        result = json.loads(machine.succeed(
            "printf '%s' '" + encoded + "' "
            f"| ${pkgs.python3}/bin/python3 ${rawHttpProbe} {target}"
        ))
        response = base64.b64decode(result["response"])
        status = None
        if response:
            first_line = response.split(b"\r\n", 1)[0]
            match = re.fullmatch(rb"HTTP/1\.[01] ([0-9]{3}) .+", first_line)
            assert match is not None, (request, response, result)
            status = int(match.group(1))
        assert result["timed_out"] is False, (request, response, result)
        assert result["reached_eof"] is True, (request, response, result)
        return status, response

    def _raw_gateway(request):
        return _raw_http(request, "gateway")

    def _raw_public(request):
        return _raw_http(request, "public")

    def _print_gateway_diagnostics(label):
        print(f"gateway diagnostics: {label}")
        print(machine.succeed(
            "systemctl status --no-pager nginx.service "
            "cratedigger-web.socket cratedigger-web.service || true"
        ))
        print(machine.succeed(
            "journalctl -u nginx.service -u cratedigger-web.service "
            "--no-pager -n 80 || true"
        ))
        print(machine.succeed(
            "ls -la /run/cratedigger-web || true; "
            "${pkgs.nginx}/bin/nginx -T "
            "-c /etc/nginx/nginx.conf 2>&1 || true"
        ))

    # Qualify the response-header checker itself: prefixed/suffixed decoy names
    # and duplicate exact names must not satisfy an exact singleton contract.
    _decoy_headers = (
        "X-Content-Security-Policy: frame-ancestors 'none'\r\n"
        "Content-Security-Policy-Decoy: frame-ancestors 'none'\r\n"
    )
    assert _response_header_values(
        _decoy_headers, "Content-Security-Policy",
    ) == []
    assert _response_header_values(
        "X-Frame-Options: DENY\r\nX-Frame-Options: DENY\r\n",
        "X-Frame-Options",
    ) == ["DENY", "DENY"]

    machine.start()
    machine.wait_for_unit("postgresql.service")
    machine.wait_for_unit("redis-cratedigger.service")
    machine.wait_for_unit("cratedigger-db-migrate.service")

    # The migrator is a oneshot with RemainAfterExit=true — confirm it landed
    # in active (exited), not failed.
    state = machine.succeed("systemctl is-active cratedigger-db-migrate.service").strip()
    assert state == "active", f"migrator unit not active: {state}"

    # The deployment-owned readiness unit supplies every mutable Beets
    # authority before any guarded application.  Cratedigger contributes no
    # renderer, Beets storage tmpfiles rule, or mutable config under stateDir.
    machine.wait_for_unit("cratedigger-test-config-hold.service")
    machine.succeed("test -f /run/cratedigger-test-config-hold")
    machine.wait_for_unit("cratedigger-test-beets-readiness.service")
    state = machine.succeed(
        "systemctl is-active cratedigger-test-beets-readiness.service"
    ).strip()
    assert state == "active", f"external Beets readiness not active: {state}"
    readiness_log = machine.succeed(
        "journalctl -b -u cratedigger-test-beets-readiness.service -o cat"
    )
    assert readiness_log.count("BEETS_EXTERNAL_READINESS_OK") == 1, readiness_log
    machine.fail("systemctl cat cratedigger-config-render.service")
    machine.fail("test -e /var/lib/cratedigger/config.ini")
    machine.fail("test -e /var/lib/cratedigger/beets")
    machine.succeed("test -f ${externalLibraryDb}")
    machine.succeed("test -f ${externalStateFile}")
    machine.succeed("test -f ${externalSecretInclude}")
    machine.succeed("test -f ${externalBeetsConfigDir}/config.yaml")
    # The deployment identity deliberately owns write authority at the Unix
    # permission layer. The main unit's denial probe therefore proves its
    # service-local read-only bind mounts, not an incidental mode-bit denial.
    machine.succeed(
        "runuser -u cratedigger -- touch "
        "${externalLibraryRoot}/.outside-main-sandbox; "
        "runuser -u cratedigger -- touch "
        "${externalLibraryDbParent}/.outside-main-sandbox; "
        "rm ${externalLibraryRoot}/.outside-main-sandbox "
        "${externalLibraryDbParent}/.outside-main-sandbox"
    )
    machine.succeed(
        "systemd-tmpfiles --cat-config > /tmp/all-tmpfiles; "
        "! grep -E '^[^#]* (/var/lib/cratedigger-music/Beets|"
        "/var/lib/cratedigger-beets-db|/var/lib/cratedigger-beets-state|"
        "/run/cratedigger-test-beets)( |$)' /tmp/all-tmpfiles"
    )

    for service in (
        "cratedigger.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-web.service",
    ):
        machine.succeed(
            f"systemctl show -p After {service} "
            "| grep -qw cratedigger-test-beets-readiness.service"
        )
        if service == "cratedigger.service":
            machine.succeed(
                f"systemctl show -p Wants {service} "
                "| grep -qw cratedigger-test-beets-readiness.service"
            )
            machine.fail(
                f"systemctl show -p Requires {service} "
                "| grep -qw cratedigger-test-beets-readiness.service"
            )
        else:
            machine.succeed(
                f"systemctl show -p Requires {service} "
                "| grep -qw cratedigger-test-beets-readiness.service"
            )
        machine.fail(
            f"systemctl show -p ExecStartPre {service} "
            "| grep -q cratedigger-check-beets-config"
        )

    # Immutable config exists while every application remains held, and no
    # independent deployment action disturbs the main singleton lock.
    machine.succeed("printf 'active-cycle\\n' > /var/lib/cratedigger/.cratedigger.lock")
    machine.succeed("grep -qx 'active-cycle' /var/lib/cratedigger/.cratedigger.lock")
    machine.fail("systemctl cat cratedigger-importer.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-import-preview-worker.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-unfindable.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-youtube-ingest.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-web.service | grep -q cratedigger-pipeline-prestart")
    machine.succeed("systemctl cat cratedigger.service | grep -q cratedigger-pipeline-prestart")

    # The deploy-hold helper verifies this independently deployed boundary
    # before it mutates systemd. Keep the synthetic downstream fixture shaped
    # exactly like production while composing its first-boot config hold.
    controlled_start_conditions = {
        "cratedigger.service": (
            "ConditionPathExists="
            "!/var/lib/cratedigger-metadata-gate/inhibit-cratedigger.service"
        ),
        "cratedigger-youtube-ingest.service": (
            "ConditionPathExists="
            "!/var/lib/cratedigger-metadata-gate/"
            "inhibit-cratedigger-youtube-ingest.service"
        ),
    }
    for service, condition in controlled_start_conditions.items():
        source = machine.succeed(f"systemctl cat {service}")
        assert source.splitlines().count(condition) == 1, (service, source)
        execution = machine.succeed(
            f"systemctl show {service} --property=ExecCondition --value"
        )
        assert "cratedigger-metadata-gate" in execution, (service, execution)
        assert "cratedigger-test-config-hold" in execution, (service, execution)

    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
    ):
        source = machine.succeed(f"systemctl cat {service}")
        for condition in controlled_start_conditions.values():
            inhibitor = condition.removeprefix("ConditionPathExists=!")
            assert inhibitor not in source, (service, inhibitor, source)
        execution = machine.succeed(
            f"systemctl show {service} --property=ExecCondition --value"
        )
        assert "cratedigger-metadata-gate" in execution, (service, execution)
        assert "cratedigger-test-config-hold" in execution, (service, execution)

    # Qualify the helper's exact-singleton checker against real systemd. A
    # duplicated drop-in condition must fail before a deployment receipt or
    # any other hold state is created.
    machine.succeed(
        "install -d /run/systemd/system/cratedigger.service.d"
    )
    machine.succeed(
        "printf '[Unit]\\nConditionPathExists="
        "!/var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger.service\\n' "
        "> /run/systemd/system/cratedigger.service.d/"
        "duplicate-inhibitor.conf"
    )
    machine.succeed("systemctl daemon-reload")
    duplicate_status, duplicate_output = machine.execute(
        "timeout 10 cratedigger-deploy-hold acquire 2>&1"
    )
    assert duplicate_status != 0, duplicate_output
    assert (
        "controlled-start prerequisite changed for cratedigger.service"
        in duplicate_output
    ), duplicate_output
    machine.fail("test -e /run/cratedigger-deploy-hold")
    machine.succeed(
        "rm -r /run/systemd/system/cratedigger.service.d"
    )
    machine.succeed("systemctl daemon-reload")

    # The production hold's lifecycle preflight reads the root-only pgpass
    # environment file before invoking pipeline-cli. The VM uses peer auth,
    # so the value is intentionally synthetic while the boundary is real.
    machine.succeed("install -d -o root -g root -m 0700 /run/secrets")
    machine.succeed(
        "printf 'PGPASSWORD=module-vm-unused\\n' "
        "> /run/secrets/cratedigger-pgpass"
    )
    machine.succeed(
        "chmod 0400 /run/secrets/cratedigger-pgpass"
    )

    # Migrations recorded
    out = machine.succeed("sudo -u postgres psql cratedigger -At -c 'SELECT version FROM schema_migrations ORDER BY version'")
    versions = [v.strip() for v in out.strip().split() if v.strip()]
    assert "1" in versions, f"baseline migration missing, got {versions}"
    assert "2" in versions, f"002 migration missing, got {versions}"

    # #750: NixOS materialises generated units in /etc/systemd/system, which
    # outranks the ordinary runtime-mask location. Reproduce the real failure:
    # /run/systemd/system/<timer> -> /dev/null exists, yet the unit remains
    # loaded from /etc and an already-queued service start survives.
    machine.succeed("test -L /etc/systemd/system/cratedigger.timer")
    machine.succeed("touch /run/cratedigger-deploy-hold-blocker")
    machine.succeed("systemctl start --no-block cratedigger-deploy-hold-blocker.service")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=MainPID --value | grep -Ev '^(0)?$'")
    machine.succeed("systemctl start --no-block cratedigger.service")
    queued_job = machine.wait_until_succeeds("systemctl show cratedigger.service --property=Job --value | grep -E '^[0-9]+$'").strip()
    queued_state = machine.succeed(f"systemctl show {queued_job} --property=State --value").strip()
    assert queued_state == "waiting", f"expected queued start job, got {queued_state}"
    machine.succeed("systemctl mask --runtime cratedigger.timer")
    machine.succeed("test \"$(readlink /run/systemd/system/cratedigger.timer)\" = /dev/null")
    machine.succeed("systemctl daemon-reload")
    load_state = machine.succeed("systemctl show cratedigger.timer --property=LoadState --value").strip()
    assert load_state == "loaded", f"ordinary runtime mask unexpectedly won: {load_state}"
    machine.succeed(f"test \"$(systemctl show cratedigger.service --property=Job --value)\" = {queued_job}")
    machine.succeed(f"systemctl cancel {queued_job}")
    machine.succeed("systemctl unmask --runtime cratedigger.timer")
    machine.succeed("systemctl daemon-reload")
    machine.succeed("rm /run/cratedigger-deploy-hold-blocker")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=ActiveState --value | grep -qx inactive")

    # Exercise the reviewed helper against real systemd. Queue two exact
    # services behind the blocker and leave the watchdog in a job-free terminal
    # failure; acquire must cancel only waiting starts, reset only the terminal
    # failure, mask only the three timers through system.control, and reach
    # stable inactivity before it returns.
    machine.succeed("install -d /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d")
    machine.succeed("printf '[Service]\\nExecStart=\\nExecStart=/run/current-system/sw/bin/false\\n' > /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d/fail.conf")
    machine.succeed("systemctl daemon-reload")
    machine.fail("systemctl start cratedigger-metadata-gate-watchdog.service")
    machine.succeed("systemctl show cratedigger-metadata-gate-watchdog.service --property=ActiveState --value | grep -qx failed")
    machine.succeed("touch /run/cratedigger-deploy-hold-blocker")
    machine.succeed("systemctl start --no-block cratedigger-deploy-hold-blocker.service")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=MainPID --value | grep -Ev '^(0)?$'")
    for service in (
        "cratedigger.service",
        "cratedigger-unfindable.service",
    ):
        machine.succeed(f"systemctl start --no-block {service}")
        job = machine.wait_until_succeeds(f"systemctl show {service} --property=Job --value | grep -E '^[0-9]+$'").strip()
        state = machine.succeed(f"systemctl show {job} --property=State --value").strip()
        assert state == "waiting", f"{service} job was not waiting: {state}"

    # #1078: acquire now drains producers and controlled workers in two
    # separate passes (each its own two-stable-sample proof) around a
    # queue-drain wait that itself round-trips pipeline-cli at least once
    # more than before the reorder. 120s keeps comfortable headroom over the
    # measured few extra seconds this adds.
    acquire_status, acquire_output = machine.execute("timeout 120 cratedigger-deploy-hold acquire")
    if acquire_status != 0:
        print(acquire_output)
        print(machine.succeed("systemctl list-jobs --no-legend || true"))
        for service in (
            "cratedigger.service",
            "cratedigger-unfindable.service",
            "cratedigger-metadata-gate-watchdog.service",
        ):
            print(machine.succeed(f"systemctl show {service} --property=Job --property=LoadState --property=ActiveState --property=SubState"))
    assert acquire_status == 0, f"deploy hold acquire failed: {acquire_status}"
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(f"test \"$(readlink /run/systemd/system.control/{timer})\" = /dev/null")
        state = machine.succeed(f"systemctl show {timer} --property=LoadState --value").strip()
        assert state == "masked", f"{timer} not authoritatively masked: {state}"
    for service in (
        "cratedigger.service",
        "cratedigger-unfindable.service",
        "cratedigger-metadata-gate-watchdog.service",
    ):
        machine.succeed(f"test -z \"$(systemctl show {service} --property=Job --value)\"")
        state = machine.succeed(f"systemctl show {service} --property=ActiveState --value").strip()
        assert state == "inactive", f"{service} not inactive after hold: {state}"

    # Qualify idempotent post-switch verification and the staged release.
    # First boot already proved the independent config hold. Remove only that
    # synthetic prerequisite so the production-shaped metadata gate can start
    # and observe the controlled workers during release.
    machine.succeed("rm -r /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d")
    machine.succeed("systemctl daemon-reload")
    machine.succeed("cratedigger-deploy-hold verify-held")
    machine.succeed("rm /run/cratedigger-deploy-hold-blocker")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=ActiveState --value | grep -qx inactive")
    machine.succeed("rm /run/cratedigger-test-config-hold")
    machine.succeed("cratedigger-deploy-hold prepare-controlled")
    machine.succeed("cratedigger-deploy-hold open-main-timer")
    machine.succeed("cratedigger-deploy-hold finish-release aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.succeed("cratedigger-deploy-hold complete aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.fail("test -e /run/cratedigger-deploy-hold")
    # #1078: this previously checked /run/cratedigger-metadata-gate/holds/manual,
    # a path nothing under this tool ever writes (the real state dir is
    # metadataGateStateDir = /var/lib/cratedigger-metadata-gate) -- the
    # assertion could never fail and proved nothing about the released hold.
    machine.fail("test -e /var/lib/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.fail(f"test -e /run/systemd/system.control/{timer}")
        state = machine.succeed(f"systemctl show {timer} --property=LoadState --value").strip()
        assert state == "loaded", f"{timer} not restored after release: {state}"

    # The timer-owned main service must survive both a healthy restart and a
    # failed restart of an external readiness producer. Hold one live
    # invocation in ExecStartPre and retain exactly the same invocation rather
    # than merely observing that a later cycle happens to run. Stop the
    # long-running workers first so the worlds below can separately prove the
    # main service's soft edge and their hard Requires= edges.
    machine.succeed(
        "systemctl stop cratedigger-importer.service "
        "cratedigger-import-preview-worker.service cratedigger-web.service"
    )
    machine.succeed(
        "install -d /run/systemd/system/cratedigger.service.d; "
        "printf '[Service]\\nRestart=no\\nExecStartPre=/run/current-system/sw/bin/sleep 30\\n' "
        "> /run/systemd/system/cratedigger.service.d/hold-main.conf; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger.service; "
        "systemctl start --no-block cratedigger.service"
    )
    machine.wait_until_succeeds(
        "systemctl show cratedigger.service -p ActiveState --value | grep -qx activating; "
        "systemctl show cratedigger.service -p InvocationID --value | grep -Eq '^[0-9a-f]{32}$'"
    )
    main_invocation_before_readiness_restart = machine.succeed(
        "systemctl show cratedigger.service -p InvocationID --value"
    ).strip()
    machine.succeed("systemctl restart cratedigger-test-beets-readiness.service")
    machine.wait_for_unit("cratedigger-test-beets-readiness.service")
    main_invocation_after_readiness_restart = machine.succeed(
        "systemctl show cratedigger.service -p InvocationID --value"
    ).strip()
    assert main_invocation_after_readiness_restart == main_invocation_before_readiness_restart, (
        main_invocation_before_readiness_restart,
        main_invocation_after_readiness_restart,
    )
    machine.succeed("touch /run/cratedigger-test-beets-readiness-fail")
    machine.fail("systemctl restart cratedigger-test-beets-readiness.service")
    machine.wait_until_succeeds(
        "systemctl is-failed cratedigger-test-beets-readiness.service"
    )
    main_invocation_after_failed_readiness_restart = machine.succeed(
        "systemctl show cratedigger.service -p InvocationID --value"
    ).strip()
    assert (
        main_invocation_after_failed_readiness_restart
        == main_invocation_before_readiness_restart
    ), (
        main_invocation_before_readiness_restart,
        main_invocation_after_failed_readiness_restart,
    )
    machine.succeed(
        "systemctl show cratedigger.service -p ActiveState --value "
        "| grep -qx activating"
    )
    machine.succeed(
        "systemctl stop cratedigger.service; "
        "rm -r /run/systemd/system/cratedigger.service.d; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger.service"
    )

    # A fresh main start has only Wants=+After= on readiness. Even though the
    # producer remains failed, the soft dependency must let intrinsic Beets
    # admission run. The service will later fail because there is no real
    # slskd, which is outside this contract.
    machine.succeed("systemctl start --no-block cratedigger.service")
    machine.wait_until_succeeds("test ! -f /var/lib/cratedigger/.cratedigger.lock")
    machine.wait_until_succeeds(
        "journalctl -b -u cratedigger.service -o cat "
        "| grep -q 'Beets configuration admitted for main'"
    )
    machine.succeed(
        "systemctl kill --kill-whom=all --signal=SIGKILL cratedigger.service || true"
    )
    machine.succeed("systemctl reset-failed cratedigger.service || true")

    # Mutation/observer workers retain Requires=+After=. With readiness still
    # forced to fail, each fresh start must be rejected before its intrinsic
    # admission path executes.
    hard_readiness_units = (
        ("importer", "cratedigger-importer.service"),
        ("preview", "cratedigger-import-preview-worker.service"),
        ("web", "cratedigger-web.service"),
    )
    admissions_before_failed_readiness = {}
    for role, unit in hard_readiness_units:
        startup_log = machine.succeed(f"journalctl -b -u {unit} -o cat")
        admissions_before_failed_readiness[unit] = startup_log.count(
            f"Beets configuration admitted for {role}"
        )
        machine.fail(f"systemctl start {unit}")
        machine.fail(f"systemctl is-active {unit}")
        startup_log = machine.succeed(f"journalctl -b -u {unit} -o cat")
        assert startup_log.count(
            f"Beets configuration admitted for {role}"
        ) == admissions_before_failed_readiness[unit], (unit, startup_log)
    readiness_failure_log = machine.succeed(
        "journalctl -b -u cratedigger-test-beets-readiness.service -o cat"
    )
    assert "BEETS_EXTERNAL_READINESS_FAILED" in readiness_failure_log, (
        readiness_failure_log
    )

    # Once deployment restores readiness, all hard-dependent workers recover
    # through their ordinary starts and reach intrinsic admission exactly once.
    machine.succeed(
        "rm /run/cratedigger-test-beets-readiness-fail; "
        "systemctl reset-failed cratedigger-test-beets-readiness.service "
        "cratedigger-importer.service "
        "cratedigger-import-preview-worker.service cratedigger-web.service; "
        "systemctl start cratedigger-test-beets-readiness.service"
    )
    machine.wait_for_unit("cratedigger-test-beets-readiness.service")
    machine.succeed("systemctl start cratedigger-importer.service cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service cratedigger-web.service")
    for role, unit in (
        ("importer", "cratedigger-importer.service"),
        ("preview", "cratedigger-import-preview-worker.service"),
        ("web", "cratedigger-web.service"),
    ):
        machine.wait_until_succeeds(
            f"journalctl -b -u {unit} -o cat "
            f"| grep -c 'Beets configuration admitted for {role}' "
            "| grep -qx 2"
        )
    for role, unit in (
        ("main", "cratedigger.service"),
        ("importer", "cratedigger-importer.service"),
        ("preview", "cratedigger-import-preview-worker.service"),
        ("web", "cratedigger-web.service"),
    ):
        startup_log = machine.succeed(
            f"journalctl -b -u {unit} -o cat"
        )
        expected_admissions = 1 if role == "main" else 2
        assert startup_log.count(
            f"Beets configuration admitted for {role}"
        ) == expected_admissions, (unit, startup_log)
    runtime_config = machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| awk 'seen { print; exit } $0 == \"--config\" { seen=1 }'"
    ).strip()
    assert runtime_config.startswith("/nix/store/"), runtime_config
    machine.succeed(f"test -f {runtime_config}")
    machine.fail(f"runuser -u cratedigger -- sh -c 'printf x >> {runtime_config}'")

    # #1098: the deploy-hold scenario above never gets the controlled
    # workers running -- every app unit stays behind the synthetic
    # first-boot config hold the whole time -- so it cannot exercise
    # abort's real restart proof or its foreign-hold refusal. The four
    # workers are genuinely active right now (started just above); reuse
    # that live world to drive a genuinely incomplete acquire, then abort
    # against it.
    #
    # A dirty old-lifecycle preflight (one automation_import job stuck in
    # recovery_required) is the mechanism: recovery_required is an anomaly
    # _wait_automation_queue_drained never drains, so acquire short-circuits
    # straight past the queue-drain wait to _assert_clean_old_lifecycle and
    # fails there -- after it has already taken the hold -- leaving the
    # receipt in PHASE_ACQUIRING with the manual hold owned and the four
    # gate-stopped units down, exactly the world abort exists to escape.
    #
    # The synthetic metadata-gate fixture (metadataGateTool, above) only
    # records the hold marker file; unlike the real deployment-owned gate
    # tool it never stops already-running units. Stop the four controlled
    # workers directly so acquire's own drain proves them inactive quickly,
    # instead of timing out waiting for a stop nothing in this fixture ever
    # performs. Two consequences of that divergence carry through the rest
    # of this scenario: (1) the SERVICE_UNITS drain inside acquire observes
    # a test-arranged down-state, not one the real gate tool produced, so
    # this scenario is not evidence that a real "hold manual" call itself
    # stops running units -- only that acquire/abort correctly wait for and
    # verify whatever state the units are actually in; (2) below,
    # "resume-if-clear" is only ever proven CALLED and reporting clean
    # (exit 0, no holds left) -- the synthetic tool's resume-if-clear is
    # purely an emptiness check on holds/ and never itself starts a unit,
    # so this scenario proves nothing about whether the real gate's
    # resume-if-clear actually restarts cratedigger.service via its
    # resume_units list.
    machine.succeed(
        "systemctl stop cratedigger-web.service cratedigger-importer.service "
        "cratedigger-import-preview-worker.service "
        "cratedigger-youtube-ingest.service"
    )

    # Seed the anomaly only now that the importer is stopped.
    # recover_abandoned_automation_owners (scripts/importer.py, run at
    # importer startup and every AUTOMATION_RECOVERY_REPROBE_INTERVAL_SECONDS
    # = 300s) converges any leaseless recovery_required automation_import
    # job on its own -- "never_claimed" is its exact death proof -- so a
    # live importer races this seed and can silently recover it away before
    # acquire ever observes it. Seeding after the stop above removes that
    # race entirely. It also means this dirty-preflight shape is
    # deterministic only while the importer stays stopped, not forever:
    # once abort restarts it later in this scenario, the same sweep would
    # converge a fresh copy of this shape on its own, so cleanup below
    # deletes the rows outright rather than leaving converged debris behind.
    #
    # Multiple semicolon-separated statements in one `psql -c` invocation
    # already share a single implicit Postgres transaction (the simple-query
    # protocol wraps them), so migration 066's deferred owner-integrity
    # constraint triggers -- which only check at COMMIT -- see the fully
    # seeded, self-consistent end state here regardless of the explicit
    # BEGIN/COMMIT below. That BEGIN/COMMIT documents to a future editor
    # that these statements must stay in one transaction -- splitting them
    # across separate `-c` invocations would trip
    # `enforce_complete_processing_owner` again, which is this scenario's
    # actual first-draft bug: seeding the job and its owning request as
    # three separate `psql -c` calls tripped it on the intermediate,
    # single-statement-committed state every time (verified against a
    # throwaway ephemeral-PG instance before writing this).
    abort_vm_seed_mbid = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "BEGIN; "
        "INSERT INTO album_requests "
        "(mb_release_id, artist_name, album_title, source, status) VALUES "
        f"('{abort_vm_seed_mbid}', 'VM Abort Artist', 'VM Abort Album', "
        "'request', 'wanted'); "
        "INSERT INTO import_jobs "
        "(job_type, status, request_id, payload, preview_status) "
        "SELECT 'automation_import', 'recovery_required', id, "
        "'{}'::jsonb, 'waiting' FROM album_requests "
        f"WHERE mb_release_id = '{abort_vm_seed_mbid}'; "
        "UPDATE album_requests SET status = 'processing', "
        "active_automation_import_job_id = (SELECT id FROM import_jobs "
        "WHERE request_id = (SELECT id FROM album_requests WHERE "
        f"mb_release_id = '{abort_vm_seed_mbid}')) "
        f"WHERE mb_release_id = '{abort_vm_seed_mbid}'; "
        "COMMIT;\""
    )
    abort_vm_request_id = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        f"SELECT id FROM album_requests WHERE mb_release_id = "
        f"'{abort_vm_seed_mbid}'\""
    ).strip()
    abort_vm_job_id = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        f"SELECT id FROM import_jobs WHERE request_id = "
        f"{abort_vm_request_id}\""
    ).strip()

    abort_acquire_status, abort_acquire_output = machine.execute(
        "timeout 120 cratedigger-deploy-hold acquire 2>&1"
    )
    assert abort_acquire_status != 0, abort_acquire_output
    assert (
        "old lifecycle is not clean for migration" in abort_acquire_output
    ), abort_acquire_output

    # Intermediate world: the receipt exists in PHASE_ACQUIRING, owning an
    # active manual hold, with all four gate-stopped units down.
    machine.succeed("test -e /run/cratedigger-deploy-hold")
    abort_phase = machine.succeed(
        "cat /run/cratedigger-deploy-hold/phase"
    ).strip()
    assert abort_phase == "acquiring", abort_phase
    machine.succeed("test -f /run/cratedigger-deploy-hold/owned-manual-hold")
    machine.succeed("test -f /var/lib/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(
            f"test -f /run/cratedigger-deploy-hold/owned-link-{timer}"
        )
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "inactive", (service, state)

    # #1078 MUST FIX 4 -- validate-before-mutate: a foreign gate hold (the
    # 2026-08-02 discogs-import outage shape) is refused by
    # _validate_no_unowned_deploy_hold_conflicts before abort mutates
    # anything at all, so every owned object is untouched by construction,
    # not merely "restored" after a partial attempt. This is a narrower,
    # different invariant than the issue's "fails partway leaves ownership
    # intact" contract -- proven below by fault injection -- because this
    # block never gets far enough to touch ownership in the first place.
    machine.succeed(
        "touch /var/lib/cratedigger-metadata-gate/holds/discogs-import"
    )

    # The reason this needed real systemd at all: "a systemctl start that a
    # gate-guarded unit's ExecCondition silently skips still returns
    # success". Prove that silent no-op directly, independent of abort --
    # cratedigger-web.service is one of metadataGateServiceNames, so its
    # ExecCondition (start-check) fails while the foreign hold sits in
    # holds/, and a condition failure is a skipped start, not a job
    # failure: systemctl still exits 0 while the unit never activates.
    machine.succeed("systemctl start cratedigger-web.service")
    web_state_under_foreign_hold = machine.succeed(
        "systemctl show cratedigger-web.service --property=ActiveState --value"
    ).strip()
    assert web_state_under_foreign_hold == "inactive", web_state_under_foreign_hold

    # Issue requirement 2 -- abort with a foreign gate hold present fails
    # loudly rather than exiting 0 with workers still down.
    foreign_abort_status, foreign_abort_output = machine.execute(
        "timeout 60 cratedigger-deploy-hold abort 2>&1"
    )
    assert foreign_abort_status != 0, foreign_abort_output
    assert (
        "foreign metadata gate holds block abort" in foreign_abort_output
    ), foreign_abort_output
    machine.succeed("test -e /run/cratedigger-deploy-hold")
    machine.succeed("test -f /run/cratedigger-deploy-hold/owned-manual-hold")
    machine.succeed("test -f /var/lib/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(
            f"test -f /run/cratedigger-deploy-hold/owned-link-{timer}"
        )
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "inactive", (service, state)

    machine.succeed(
        "rm /var/lib/cratedigger-metadata-gate/holds/discogs-import"
    )

    # Issue requirement 3 -- abort that fails partway leaves ownership
    # intact, the #1078 disown-before-restart hazard this module fixed.
    # Break the preview worker's real ExecStart (not web -- web is fronted
    # by cratedigger-web.socket, module.nix:2294-2326, a different restart
    # shape from the other three) so abort_hold's manual-hold branch gets
    # genuinely partway through: it releases the manual gate hold, starts
    # all four GATE_STOPPED_UNITS (web/importer/youtube come up for real;
    # preview flaps forever against /bin/false), then blocks inside
    # _wait_controlled_workers_active waiting for preview to stabilize --
    # which it never will. A bounded `timeout` SIGTERMs abort mid-wait,
    # after the hold was released but before unmark_manual_hold_owned()
    # (the last step of that branch) ever runs. This is the one world that
    # discriminates both mutants review found: delete the
    # _wait_controlled_workers_active call and abort exits 0 well inside
    # the bound with preview still down; move unmark_manual_hold_owned()
    # ahead of the restart proof and the owned-manual-hold marker is gone
    # by the time the kill lands, instead of surviving it.
    machine.succeed(
        "install -d /run/systemd/system/"
        "cratedigger-import-preview-worker.service.d"
    )
    machine.succeed(
        "printf '[Service]\\nExecStart=\\n"
        "ExecStart=/run/current-system/sw/bin/false\\n' > "
        "/run/systemd/system/cratedigger-import-preview-worker.service.d/"
        "fail.conf"
    )
    machine.succeed("systemctl daemon-reload")
    partial_abort_status, partial_abort_output = machine.execute(
        "timeout 20 cratedigger-deploy-hold abort 2>&1"
    )
    assert partial_abort_status != 0, partial_abort_output
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/holds/manual"
    )
    machine.succeed("test -e /run/cratedigger-deploy-hold")
    machine.succeed("test -f /run/cratedigger-deploy-hold/owned-manual-hold")
    partial_abort_phase = machine.succeed(
        "cat /run/cratedigger-deploy-hold/phase"
    ).strip()
    assert partial_abort_phase == "acquiring", partial_abort_phase
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(
            f"test -f /run/cratedigger-deploy-hold/owned-link-{timer}"
        )

    machine.succeed(
        "rm -r /run/systemd/system/"
        "cratedigger-import-preview-worker.service.d"
    )
    machine.succeed("systemctl daemon-reload")
    machine.succeed(
        # 20s of Restart=on-failure flapping against /bin/false can trip
        # the unit's own start-rate limit; clear it before trusting a fresh
        # start below.
        "systemctl reset-failed cratedigger-import-preview-worker.service"
    )

    # The rerun genuinely finishes the job the fault-injected abort above
    # left partway through -- the ownership it preserved is what makes this
    # retry possible.
    machine.succeed("timeout 120 cratedigger-deploy-hold abort")

    # Issue requirement 1 -- ordinary operation is genuinely restored, not
    # merely reported restored. abort_hold only returns after its own
    # _wait_controlled_workers_active / _assert_load_states proofs, so an
    # immediate read proves genuine restoration without tolerating an early
    # return the way wait_for_unit's polling would.
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "active", (service, state)
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        state = machine.succeed(
            f"systemctl show {timer} --property=ActiveState --value"
        ).strip()
        assert state == "active", (timer, state)
    machine.succeed("cratedigger-metadata-gate start-check")
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/holds/manual"
    )
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(f"test ! -e /run/systemd/system.control/{timer}")

    # Cleanup: drop the seeded anomaly in one explicit transaction (see the
    # seeding comment above for why one multi-statement `-c` invocation is
    # both sufficient and how this scenario's first draft got it wrong).
    machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "BEGIN; "
        "UPDATE album_requests SET status = 'wanted', "
        "active_automation_import_job_id = NULL "
        f"WHERE id = {abort_vm_request_id}; "
        f"DELETE FROM import_jobs WHERE id = {abort_vm_job_id}; "
        f"DELETE FROM album_requests WHERE id = {abort_vm_request_id}; "
        "COMMIT;\""
    )

    # CD-SEC-04: the four long-running services which process untrusted
    # network/media input share a portable hardening baseline, while each
    # retains only the writable roots its real workflow needs.  This checks
    # systemd's rendered properties, not the Nix source shape.
    def _unit_properties(unit):
        out = machine.succeed(
            f"systemctl show {unit} --no-pager "
            "--property=NoNewPrivileges --property=PrivateTmp "
            "--property=ProtectSystem --property=ProtectHome "
            "--property=RestrictAddressFamilies --property=SystemCallFilter "
            "--property=ReadWritePaths --property=BindReadOnlyPaths "
            "--property=BindPaths"
        )
        return dict(line.split("=", 1) for line in out.splitlines())

    def _assert_sandbox_properties(
        unit, properties, expected_paths,
        expected_read_only=None, expected_binds=None,
    ):
        assert properties["NoNewPrivileges"] == "yes", (unit, properties)
        assert properties["PrivateTmp"] == "yes", (unit, properties)
        assert properties["ProtectSystem"] == "strict", (unit, properties)
        assert properties["ProtectHome"] == "yes", (unit, properties)
        assert set(properties["RestrictAddressFamilies"].split()) == {
            "AF_UNIX", "AF_INET", "AF_INET6",
        }, (unit, properties)
        # systemctl expands the @system-service shorthand when reporting the
        # effective filter. Pin representative workflow syscalls and a small
        # set of prohibited privileged syscalls instead of relying on how
        # systemd chooses to render the shorthand.
        allowed_syscalls = set(properties["SystemCallFilter"].split())
        assert {"execve", "fchownat", "openat", "socket", "unlinkat"} <= allowed_syscalls, (
            unit, properties,
        )
        assert not allowed_syscalls.intersection({
            "bpf", "kexec_load", "mount", "ptrace", "reboot", "umount2",
        }), (unit, properties)
        assert set(properties["ReadWritePaths"].split()) == set(expected_paths), (
            unit, properties,
        )
        def _mount_sources(value):
            return {entry.split(":", 1)[0] for entry in value.split()}

        if expected_read_only is not None:
            assert _mount_sources(properties["BindReadOnlyPaths"]) == set(
                expected_read_only
            ), (unit, properties)
        if expected_binds is not None:
            assert _mount_sources(properties["BindPaths"]) == set(expected_binds), (
                unit, properties,
            )

    def _assert_sandbox_contract(
        unit, expected_paths, expected_read_only=(), expected_binds=(),
    ):
        _assert_sandbox_properties(
            unit, _unit_properties(unit), expected_paths,
            expected_read_only, expected_binds,
        )
        rendered_lines = machine.succeed(f"systemctl cat {unit}").splitlines()
        assert "SystemCallFilter=@system-service" in rendered_lines, (
            unit, rendered_lines,
        )

    # Checker qualification: a disabled protection must fail the same checker
    # before the actual rendered units are trusted.
    known_bad = _unit_properties("cratedigger-web.service")
    known_bad["NoNewPrivileges"] = "no"
    try:
        _assert_sandbox_properties("known-bad.service", known_bad, ["/ignored"])
    except AssertionError:
        pass
    else:
        raise AssertionError("sandbox checker accepted NoNewPrivileges=no")

    # The DB parent is a distinct authority. A broad music parent grant must
    # fail the same data-driven checker rather than merely being absent from
    # a source-level scan.
    known_bad = _unit_properties("cratedigger-web.service")
    known_bad["ReadWritePaths"] += " /var/lib/cratedigger-music"
    try:
        _assert_sandbox_properties(
            "known-bad-broad-parent.service", known_bad,
            [
                "/var/lib/cratedigger",
                "/var/lib/cratedigger/processing",
                "/var/lib/cratedigger-downloads",
                "-/var/lib/cratedigger-music/Beets",
                "-/var/lib/cratedigger-beets-db",
                "/var/lib/cratedigger-music/Incoming",
            ],
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("sandbox checker accepted a broad music-parent grant")

    known_bad = _unit_properties("cratedigger-web.service")
    known_bad["SystemCallFilter"] += " mount"
    try:
        _assert_sandbox_properties(
            "known-bad-syscall.service", known_bad,
            [
                "/var/lib/cratedigger",
                "/var/lib/cratedigger/processing",
                "/var/lib/cratedigger-downloads",
                "-/var/lib/cratedigger-music/Beets",
                "-/var/lib/cratedigger-beets-db",
                "/var/lib/cratedigger-music/Incoming",
            ],
        )
    except AssertionError:
        pass
    else:
        raise AssertionError("sandbox checker accepted mount syscall")

    _assert_sandbox_contract("cratedigger-web.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
        "-/var/lib/cratedigger-music/Beets",
        "-/var/lib/cratedigger-beets-db",
        "/var/lib/cratedigger-music/Incoming",
    ], [
        "-${externalBeetsConfigDir}",
        "-${externalStateFile}",
    ])
    _assert_sandbox_contract("cratedigger-importer.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
        "-/var/lib/cratedigger-music/Beets",
        "-/var/lib/cratedigger-beets-db",
        "/var/lib/cratedigger-music/Incoming",
        "/var/lib/cratedigger-music/Re-download",
        "-/var/lib/cratedigger-beets-state/state.pickle",
    ], ["-${externalBeetsConfigDir}"], ["-${externalStateFile}"])
    _assert_sandbox_contract("cratedigger-import-preview-worker.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
    ], [
        "-${externalBeetsConfigDir}",
        "-${externalStateFile}",
    ])
    _assert_sandbox_contract("cratedigger-youtube-ingest.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/youtube-ingest-temp",
        "/var/lib/cratedigger-music/Incoming",
    ])
    for unit in (
        "cratedigger.service",
        "cratedigger-unfindable.service",
        "cratedigger-db-migrate.service",
    ):
        properties = _unit_properties(unit)
        assert properties["NoNewPrivileges"] == "no", (unit, properties)
        assert properties["PrivateTmp"] == "no", (unit, properties)
        assert properties["ProtectSystem"] == "no", (unit, properties)
        assert properties["ProtectHome"] == "no", (unit, properties)
        assert properties["RestrictAddressFamilies"] == "~", (unit, properties)
        assert properties["SystemCallFilter"] == "~", (unit, properties)
        assert properties["ReadWritePaths"] == "", (unit, properties)
    main_properties = _unit_properties("cratedigger.service")
    assert {
        entry.split(":", 1)[0]
        for entry in main_properties["BindReadOnlyPaths"].split()
    } == {
        "-${externalBeetsConfigDir}",
        "-${externalStateFile}",
        "-${externalLibraryRoot}",
        "-${externalLibraryDbParent}",
    }, main_properties
    machine.succeed(
        "journalctl -b -u cratedigger.service -o cat "
        "| grep -q BEETS_MAIN_WRITE_DENIAL_OK"
    )

    # The importer probe ran inside the unit's sandbox. These pins prove every
    # configured authority root was writable while unrelated world-writable
    # locations remained effectively read-only despite their Unix modes.
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger-beets-db)\" = root:beets-library:2775")
    machine.succeed("runuser -u cratedigger -- test -w /var/lib/cratedigger-beets-db")
    machine.succeed("test \"$(stat -c %a /var/lib/cratedigger-music/unrelated)\" = 777")
    machine.fail("test -e /var/lib/cratedigger-music/unrelated/escape")
    machine.succeed("test \"$(stat -c %a /var/lib/cratedigger-world-writable)\" = 777")
    machine.fail("test -e /var/lib/cratedigger-world-writable/escape")
    machine.succeed("test -s /var/lib/cratedigger/processing/sandbox-probe/tone.mp3")
    machine.succeed(
        "test -s /var/lib/cratedigger/beets-incremental-vm.receipt; "
        "read before after config < /var/lib/cratedigger/beets-incremental-vm.receipt; "
        "test \"$before\" != \"$after\"; test -n \"$config\""
    )
    for root in (
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
        "/var/lib/cratedigger-music/Beets",
        "/var/lib/cratedigger-beets-db",
        "/var/lib/cratedigger-music/Incoming",
        "/var/lib/cratedigger-music/Re-download",
    ):
        machine.succeed(f"test -f {root}/.sandbox-probe")

    # #663: this is a real non-root service identity, not merely a rendered
    # User= value.  Its private processing descendants must be writable by it
    # and inaccessible to the unrelated VM user.
    machine.succeed("test $(id -u cratedigger) -ne 0")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing)\" = cratedigger:cratedigger:700")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing/albums)\" = cratedigger:cratedigger:700")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing/albums/failed_imports)\" = cratedigger:cratedigger:700")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing/preview)\" = cratedigger:cratedigger:700")
    machine.succeed("runuser -u cratedigger -- mkdir /var/lib/cratedigger/processing/preview/vm-nonroot-snapshot")
    machine.fail("runuser -u unrelated-user -- test -r /var/lib/cratedigger/processing/preview")
    machine.succeed("runuser -u cratedigger -- rmdir /var/lib/cratedigger/processing/preview/vm-nonroot-snapshot")
    machine.succeed("runuser -u slskd-writer -- sh -c 'printf source > /var/lib/cratedigger-downloads/vm-source.mp3'")
    machine.succeed("runuser -u cratedigger -- cat /var/lib/cratedigger-downloads/vm-source.mp3")
    machine.succeed("runuser -u cratedigger -- rm -f /var/lib/cratedigger-downloads/vm-source.mp3")
    machine.succeed("runuser -u slskd-writer -- sh -c 'printf source > /var/lib/cratedigger-downloads/vm-rename.mp3'")
    machine.fail("runuser -u slskd-writer -- test -r /var/lib/cratedigger/processing/preview")
    machine.fail("runuser -u slskd-writer -- touch /var/lib/cratedigger/processing/preview/foreign")
    machine.fail("runuser -u slskd-writer -- mv /var/lib/cratedigger-downloads/vm-rename.mp3 /var/lib/cratedigger/processing/albums/foreign")
    machine.fail("runuser -u slskd-writer -- rm /var/lib/cratedigger/processing/albums/foreign")
    machine.succeed("runuser -u slskd-writer -- rm /var/lib/cratedigger-downloads/vm-rename.mp3")
    machine.succeed("runuser -u cratedigger -- sh -c 'mkdir /var/lib/cratedigger/processing/albums/existing-canonical && printf canonical > /var/lib/cratedigger/processing/albums/existing-canonical/track.flac'")
    machine.fail("runuser -u slskd-writer -- cat /var/lib/cratedigger/processing/albums/existing-canonical/track.flac")
    machine.fail("runuser -u slskd-writer -- touch /var/lib/cratedigger/processing/albums/foreign-sibling")
    machine.fail("runuser -u slskd-writer -- touch /var/lib/cratedigger/processing/albums/existing-canonical/foreign-child")
    machine.fail("runuser -u slskd-writer -- mv /var/lib/cratedigger/processing/albums/existing-canonical /var/lib/cratedigger/processing/albums/renamed-canonical")
    machine.fail("runuser -u slskd-writer -- mv /var/lib/cratedigger/processing/albums/existing-canonical/track.flac /var/lib/cratedigger/processing/albums/existing-canonical/renamed-track.flac")
    machine.fail("runuser -u slskd-writer -- rm /var/lib/cratedigger/processing/albums/existing-canonical/track.flac")
    machine.fail("runuser -u slskd-writer -- rmdir /var/lib/cratedigger/processing/albums/existing-canonical")
    machine.succeed(
        f"awk '$0 == \"[Paths]\" {{ in_paths=1; next }} "
        "in_paths && /^\\[/ { exit } in_paths { print }' "
        f"{runtime_config} "
        "| grep -qx 'processing_dir = /var/lib/cratedigger/processing'"
    )
    # tmpfiles' age calculation includes the directory birth time.  Create a
    # genuine eight-day-old preview snapshot by temporarily moving the VM clock
    # back, then restore it before asking tmpfiles to clean.  Merely backdating
    # mtime with touch leaves btime new and does not exercise the configured
    # stale-preview cleanup rule.
    machine.succeed("now=$(date +%s); old=$((now - 8 * 24 * 60 * 60)); date -s @$old; runuser -u cratedigger -- mkdir /var/lib/cratedigger/processing/preview/preview-stale; date -s @$now")
    machine.succeed("runuser -u cratedigger -- touch /var/lib/cratedigger/processing/.preview-snapshot.lock")
    machine.succeed("systemd-tmpfiles --clean")
    machine.fail("test -d /var/lib/cratedigger/processing/preview/preview-stale")
    machine.succeed("test -f /var/lib/cratedigger/processing/.preview-snapshot.lock")
    machine.succeed("test -d /var/lib/cratedigger/processing/albums/existing-canonical")
    machine.succeed("test \"$(cat /var/lib/cratedigger/processing/albums/existing-canonical/track.flac)\" = canonical")
    # config.ini points at the out-of-band secret, never its plaintext value.
    machine.succeed(
        f"grep -q 'api_key_file = /etc/cratedigger/slskd-api-key' "
        f"{runtime_config}"
    )
    machine.fail(f"grep -q 'test-api-key-do-not-use' {runtime_config}")
    mode = machine.succeed(f"stat -c %a {runtime_config}").strip()
    assert mode == "444", f"immutable runtime config should be 0444, got {mode}"
    machine.succeed(f"grep -q 'enabled = True' {runtime_config}")
    machine.succeed(f"grep -q '\\[Quality Ranks\\]' {runtime_config}")
    machine.succeed(f"grep -q '^vorbis.transparent = 192$' {runtime_config}")
    machine.succeed(f"grep -q '^vorbis.excellent = 160$' {runtime_config}")
    machine.succeed(f"grep -q '^vorbis.good = 112$' {runtime_config}")
    machine.succeed(f"grep -q '^vorbis.acceptable = 96$' {runtime_config}")
    machine.succeed(f"grep -q '^wma.transparent = 320$' {runtime_config}")
    machine.succeed(f"grep -q '^wma.excellent = 256$' {runtime_config}")
    machine.succeed(f"grep -q '^wma.good = 192$' {runtime_config}")
    machine.succeed(f"grep -q '^wma.acceptable = 128$' {runtime_config}")
    # The store config carries the exact six externally supplied Beets
    # authorities and only the secret include path, never its token value.
    for expected_line in (
        "config_dir = ${externalBeetsConfigDir}",
        "library = ${externalLibraryDb}",
        "directory = ${externalLibraryRoot}",
        "state_file = ${externalStateFile}",
        "python = /nix/store/",
        "secret_include = ${externalSecretInclude}",
    ):
        machine.succeed(f"grep -Fq '{expected_line}' {runtime_config}")
    beets_runtime_keys = machine.succeed(
        f"awk '$0 == \"[Beets]\" {{ active=1; next }} "
        "active && /^\\[/ { exit } "
        "active && /=/ { sub(/[[:space:]]*=.*/, \"\"); print }' "
        f"{runtime_config}"
    ).splitlines()
    assert len(beets_runtime_keys) == 6, beets_runtime_keys
    assert set(beets_runtime_keys) == {
        "config_dir", "library", "directory", "state_file", "python",
        "secret_include",
    }, beets_runtime_keys
    machine.succeed(
        "token=$(sed -n 's/^  user_token: \"\\(.*\\)\"$/\\1/p' "
        "${externalSecretInclude}); test -n \"$token\"; "
        f"! grep -F \"$token\" {runtime_config}; "
        "! grep -F \"$token\" ${externalBeetsConfigDir}/config.yaml"
    )
    beets_python = machine.succeed(
        f"sed -n 's/^python = //p' {runtime_config}"
    ).strip()
    machine.succeed(
        "test \"$(readlink -f $(command -v beet))\" "
        "= ${externalBeetsPackage}/bin/beet"
    )
    machine.succeed(
        f"test \"$({beets_python} -c 'import beets; print(beets.__version__)')\" "
        "= ${externalBeetsPackage.version}"
    )
    machine.succeed(
        f"python_root={beets_python}; "
        "python_root=''${python_root%/bin/python}; "
        f"nix-store -qR \"$python_root\" | grep -Fx '${externalBeetsPackage}'"
    )
    for command in (
        "cratedigger-importer",
        "cratedigger-check-beets-config",
    ):
        machine.succeed(
            f"root=$(readlink -f $(command -v {command})); "
            "root=''${root%/bin/*}; "
            f"nix-store -qR \"$root\" | grep -Fx '${externalBeetsPackage}'"
        )
    # Run the packaged checker inside the real role namespaces so its state
    # access view matches each application rather than the host namespace.
    for role, unit in (
        ("main", "cratedigger-web.service"),
        ("web", "cratedigger-web.service"),
        ("preview", "cratedigger-import-preview-worker.service"),
        ("importer", "cratedigger-importer.service"),
    ):
        machine.succeed(
            f"pid=$(systemctl show {unit} -p MainPID --value); "
            f"nsenter -t $pid -m -- runuser -u cratedigger -- "
            f"cratedigger-check-beets-config --role {role} "
            f"| grep -q '\"ok\":true'"
        )
    machine.succeed("test -d /var/lib/cratedigger-beets-db")
    machine.succeed("test -f /var/lib/cratedigger-beets-db/.sandbox-probe")
    machine.succeed(f"grep -q 'api_base = https://musicbrainz.org' {runtime_config}")
    machine.succeed(f"grep -q '\\[Peer Cache\\]' {runtime_config}")
    machine.succeed(f"grep -q 'redis_host = 127.0.0.1' {runtime_config}")
    machine.succeed(f"grep -q 'ttl_seconds = 604800' {runtime_config}")
    machine.succeed(f"grep -q '^library_id = music-library-item-id$' {runtime_config}")
    machine.succeed("${pkgs.redis}/bin/redis-cli -p 6379 CONFIG GET maxmemory-policy | grep -q allkeys-lru")
    machine.succeed("systemctl show -p After cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p After cratedigger-web.service | grep -q redis-cratedigger.service")
    machine.succeed("systemctl show -p Wants cratedigger-web.service | grep -q redis-cratedigger.service")

    # Deploy-kill-migrate fix: cratedigger.service is timer-driven and
    # restartIfChanged=false, so it must NOT Requires= the migrate unit --
    # that unit's ExecStart store path changes on every deploy, and a
    # Requires= edge would propagate its every-switch restart as a SIGTERM
    # to a mid-flight cycle. It still Wants=+After= the migrate unit (so it
    # normally starts behind a first-boot migration) and gates on schema
    # currency itself at startup instead (lib/migrator.py
    # assert_schema_current, exercised by the Python suite).
    machine.succeed("systemctl show -p Wants cratedigger.service | grep -q cratedigger-db-migrate.service")
    machine.fail("systemctl show -p Requires cratedigger.service | grep -q cratedigger-db-migrate.service")

    # Counterpart pins: the long-running workers restart on switch anyway
    # (restartIfChanged=true), so they MUST keep the hard Requires= gate --
    # for them it's harmless AND it's their only "failed migration blocks
    # start" guarantee (they have no assert_schema_current startup gate).
    # A future edit flipping one of these to Wants= would silently lose
    # that guarantee. (youtube-ingest's identical pin lives in its U7
    # block further down.)
    machine.succeed("systemctl show -p Requires cratedigger-web.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-importer.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-import-preview-worker.service | grep -q cratedigger-db-migrate.service")

    # Peer auth by construction (KTD5): the socket DSN carries no
    # password, and none exists in the rendered config or unit files.
    machine.succeed(f"grep -q 'dsn = postgresql:///cratedigger?host=/run/postgresql' {runtime_config}")
    # (password_file *keys* are fine — they are the #117 *File pattern;
    # what must not exist is an actual credential value.)
    machine.fail(f"grep -Eqi 'password *= *[^ ]|pgpassword' {runtime_config}")
    machine.succeed(
        "systemctl show cratedigger-db-migrate -p Environment"
        " | grep -q 'PIPELINE_DB_DSN=postgresql:///cratedigger?host=/run/postgresql'"
    )

    # Module-owned first-boot ordering (U7/U10): migrate is serialised behind
    # NixOS's role/database setup oneshot; every app unit requires migrate —
    # the stranger's first boot cannot race role creation or DB ownership.
    machine.succeed("systemctl show -p After cratedigger-db-migrate.service | grep -q postgresql-setup.service")
    machine.succeed("systemctl show -p Requires cratedigger-db-migrate.service | grep -q postgresql-setup.service")

    # pipeline-cli on PATH and connects (over the peer-auth socket)
    machine.succeed("sudo -u cratedigger pipeline-cli list wanted")
    # The installed launcher must not import a same-named package from an
    # operator-controlled working directory. The hostile package would leave a
    # sentinel before aborting if Python's -c current-directory entry won.
    machine.succeed(
        "install -d -o cratedigger -g beets-library "
        "/tmp/cratedigger-hostile-cwd/scripts/pipeline_cli; "
        "touch /tmp/cratedigger-hostile-cwd/scripts/__init__.py "
        "/tmp/cratedigger-hostile-cwd/scripts/pipeline_cli/__init__.py; "
        "printf '%s\\n' "
        "'from pathlib import Path' "
        "'Path(\"/tmp/cratedigger-hostile-imported\").write_text(\"shadow\")' "
        "'raise RuntimeError(\"hostile pipeline-cli shadow executed\")' "
        "> /tmp/cratedigger-hostile-cwd/scripts/pipeline_cli/cli.py; "
        "chown -R cratedigger:beets-library /tmp/cratedigger-hostile-cwd; "
        "runuser -u cratedigger -- sh -c "
        "'cd /tmp/cratedigger-hostile-cwd && pipeline-cli list wanted'; "
        "test ! -e /tmp/cratedigger-hostile-imported"
    )
    # The differential wrapper accepts operator paths, but never operator
    # Python startup code. -I plus the script-owned repository root must win.
    machine.succeed(
        "install -d -o cratedigger -g beets-library /tmp/cratedigger-hostile-python; "
        "printf '%s\\n' 'from pathlib import Path' "
        "'Path(\"/tmp/cratedigger-hostile-sitecustomize\").write_text(\"shadow\")' "
        "> /tmp/cratedigger-hostile-python/sitecustomize.py; "
        "runuser -u cratedigger -- sh -c "
        "'PYTHONPATH=/tmp/cratedigger-hostile-python decision-differential --help >/dev/null'; "
        "test ! -e /tmp/cratedigger-hostile-sitecustomize"
    )

    # #663 U4 (Basic slice): the Python application owns only the inherited
    # Unix listener. Nginx owns the distinct loopback gateway, with no legacy
    # Python TCP port and no wildcard/public bind.
    machine.wait_for_unit("cratedigger-web.service")
    machine.wait_for_unit("nginx.service")
    machine.wait_for_open_port(18086)
    machine.wait_for_open_port(18443)
    machine.wait_for_open_port(18087)
    machine.succeed(
        "markers=$(find /run/cratedigger-web -maxdepth 1 -type f "
        "-name 'gateway-*'); "
        "test \"$(printf '%s\\n' \"$markers\" | grep -c .)\" = 1; "
        "printf '%s\\n' \"$markers\" "
        "| grep -Eq '^/run/cratedigger-web/"
        "gateway-policy-[0-9a-f]{64}$'"
    )
    machine.succeed(
        "${pkgs.nginx}/bin/nginx -T -c /etc/nginx/nginx.conf "
        "> /tmp/nginx-public-proxy 2>&1; "
        "grep -F 'listen 0.0.0.0:18443 ssl' /tmp/nginx-public-proxy; "
        "grep -F 'proxy_pass http://127.0.0.1:18086' "
        "/tmp/nginx-public-proxy; "
        "grep -F 'proxy_set_header Host music.vm.test' "
        "/tmp/nginx-public-proxy; "
        "grep -F 'proxy_set_header X-Forwarded-Proto https' "
        "/tmp/nginx-public-proxy; "
        "grep -F 'proxy_set_header X-Forwarded-Port 443' "
        "/tmp/nginx-public-proxy; "
        "grep -F 'auth_basic off' /tmp/nginx-public-proxy; "
        "grep -F 'proxy_pass_request_body off' /tmp/nginx-public-proxy; "
        "grep -F 'proxy_set_header Connection close' /tmp/nginx-public-proxy"
    )
    machine.succeed(
        "actual=$(ss -H -ltn 'sport = :18443' | awk '{print $4}' | sort); "
        "expected=$(printf '%s\\n' '0.0.0.0:18443' '[::]:18443' | sort); "
        "test \"$actual\" = \"$expected\""
    )
    machine.succeed(
        "actual=$(ss -H -ltn 'sport = :18086' | awk '{print $4}' | sort); "
        "expected=$(printf '%s\\n' '127.0.0.1:18086' '127.0.0.2:18086' "
        "'[::1]:18086' | sort); "
        "test \"$actual\" = \"$expected\""
    )
    machine.fail("ss -H -ltn 'sport = :8085' | grep -q .")
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "test \"$pid\" -gt 1; "
        "! ss -H -ltnp | grep -E \"pid=$pid([,)])\""
    )
    machine.succeed(
        "test \"$(systemctl show cratedigger-web.service "
        "-p User --value)\" = cratedigger; "
        "test \"$(systemctl show cratedigger-web.service "
        "-p Group --value)\" = cratedigger; "
        "systemctl show cratedigger-web.service -p ExecStartPre --value "
        "| grep -F cratedigger-web-basic-auth-validate; "
        "systemctl show cratedigger-web.service -p ExecStartPre --value "
        "| grep -F cratedigger-web-basic-auth-app-isolation"
    )
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -A1 -Fx -- '--canonical-origin' "
        "| tail -n1 | grep -Fx 'https://music.vm.test'"
    )

    # Parent traversal and socket-node access are independent exact
    # permissions. Nginx and the explicit operator can connect; an unrelated
    # local account is denied before it can speak HTTP.
    machine.succeed(
        "test \"$(stat -c %U:%G:%a /run/cratedigger-web)\" "
        "= root:cratedigger-web:750"
    )
    machine.succeed(
        "test \"$(stat -c %U:%G:%a /run/cratedigger-web/web.sock)\" "
        "= root:cratedigger-web:660"
    )
    machine.succeed("id -nG nginx | tr ' ' '\\n' | grep -Fx cratedigger-web")
    machine.succeed(
        "id -nG beets-operator | tr ' ' '\\n' | grep -Fx cratedigger-web"
    )
    machine.fail(
        "id -nG unrelated-user | tr ' ' '\\n' | grep -Fx cratedigger-web"
    )
    for identity in ("nginx", "beets-operator"):
        machine.succeed(
            f"runuser -u {identity} -- curl -sS --unix-socket "
            f"/run/cratedigger-web/web.sock -o /dev/null -w '%{{http_code}}' "
            "http://cratedigger.internal/healthz | grep -Fx 204"
        )
    machine.fail(
        "runuser -u unrelated-user -- test -x /run/cratedigger-web"
    )
    machine.fail(
        "runuser -u unrelated-user -- curl -sS --unix-socket "
        "/run/cratedigger-web/web.sock http://cratedigger.internal/healthz"
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/inherited-basic)\" = 401"
    )
    machine.succeed(
        "test \"$(curl -sS -D /tmp/inherited-basic-headers "
        "-o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password -H 'Host: music.vm.test' "
        "https://music.vm.test:18443/inherited-basic)\" = 404"
    )
    inherited_basic_headers = machine.succeed(
        "cat /tmp/inherited-basic-headers"
    )
    _assert_exact_response_header(
        inherited_basic_headers,
        "Content-Security-Policy",
        "frame-ancestors 'none'",
    )
    _assert_exact_response_header(
        inherited_basic_headers,
        "X-Frame-Options",
        "DENY",
    )
    _assert_exact_response_header(
        inherited_basic_headers,
        "Cross-Origin-Resource-Policy",
        "same-origin",
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' -X POST "
        "--user test-operator:test-password -H 'Host: music.vm.test' "
        "-H 'X-Cratedigger-Request-Channel: cli' "
        "https://music.vm.test:18443/inherited-basic)\" = 403"
    )
    machine.succeed("rm /tmp/inherited-basic-headers")

    # The test bcrypt credential is generated at runtime. Its configured and
    # resolved paths stay outside the store; nginx alone can read it. Neither
    # the application nor another socket-authorized operator can read it, and
    # the live hash is absent from the application's argv/environment.
    machine.succeed(
        "test \"$(realpath /run/cratedigger-test-auth/basic.htpasswd)\" "
        "= /run/cratedigger-test-auth/basic.htpasswd"
    )
    machine.succeed(
        "grep -Eq '^test-operator:\\$2[aby]\\$' "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    machine.succeed(
        "test \"$(stat -c %U:%G:%a "
        "/run/cratedigger-test-auth/basic.htpasswd)\" = root:nginx:440"
    )
    machine.succeed(
        "runuser -u nginx -- test -r "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    machine.fail(
        "runuser -u cratedigger -- test -r "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    machine.fail(
        "runuser -u beets-operator -- test -r "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    # Fault-qualify the module-owned application-identity preflight itself:
    # it succeeds for the real web user, but rejects both the credential
    # group and root identities that downstream unit overrides could select.
    machine.succeed(
        "isolation=$(systemctl cat cratedigger-web.service "
        "| sed -n 's|^ExecStartPre=\\([^ ]*"
        "cratedigger-web-basic-auth-app-isolation[^ ]*\\)$|\\1|p'); "
        "test -x \"$isolation\"; "
        "runuser -u cratedigger -- \"$isolation\"; "
        "! runuser -u nginx -- \"$isolation\"; "
        "! \"$isolation\""
    )
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "nginx_gid=$(getent group nginx | cut -d: -f3); "
        "! awk -v gid=\"$nginx_gid\" "
        "'/^Groups:/ { for (i = 2; i <= NF; i++) if ($i == gid) found = 1 } "
        "END { exit found ? 0 : 1 }' /proc/$pid/status; "
        "hash=$(cut -d: -f2 /run/cratedigger-test-auth/basic.htpasswd); "
        "! tr '\\0' '\\n' < /proc/$pid/environ | grep -F -- \"$hash\"; "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline | grep -F -- \"$hash\""
    )

    # Whole-site Basic authentication: representative document, static,
    # ordinary read, route-discovery, and mutation surfaces all deny anonymous
    # and incorrect credentials. Correct credentials cross the gateway. The
    # mutation then remains subject to the application's browser-origin check.
    protected_read_paths = (
        "/",
        "/js/main.js",
        "/api/pipeline/status",
        "/api/_index",
    )
    for path in protected_read_paths:
        machine.succeed(
            f"test \"$(curl -sS -o /dev/null -w '%{{http_code}}' "
            f"https://music.vm.test:18443{path})\" = 401"
        )
    for path in protected_read_paths:
        machine.succeed(
            f"test \"$(curl -sS -o /dev/null -w '%{{http_code}}' "
            f"--user test-operator:wrong "
            f"https://music.vm.test:18443{path})\" = 401"
        )
    for credential_args in ("", "--user test-operator:wrong"):
        machine.succeed(
            f"test \"$(curl -sS -o /dev/null -w '%{{http_code}}' "
            f"{credential_args} "
            "-H 'Content-Type: application/json' "
            "-H 'Origin: https://music.vm.test' -d '{}' "
            "https://music.vm.test:18443/api/pipeline/add)\" = 401"
        )
    for path in protected_read_paths:
        machine.succeed(
            f"test \"$(curl -sS -o /dev/null -w '%{{http_code}}' "
            f"--user test-operator:test-password "
            f"https://music.vm.test:18443{path})\" = 200"
        )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'Content-Type: application/json' "
        "-H 'Origin: https://music.vm.test' -d '{}' "
        "https://music.vm.test:18443/api/pipeline/add)\" = 400"
    )
    # A caller-supplied internal channel cannot turn a browser request into a
    # CLI request: nginx replaces it with the browser marker, so missing
    # browser provenance still fails before route dispatch.
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'X-Cratedigger-Request-Channel: cli' "
        "-H 'Content-Type: application/json' -d '{}' "
        "https://music.vm.test:18443/api/pipeline/add)\" = 403"
    )

    # Only the exact anonymous GET/HEAD liveness target is exempt. Query,
    # trailing-slash, and method variants do not acquire ordinary app access.
    for method_flag in ("", "--head"):
        machine.succeed(
            f"test \"$(curl -sS -o /dev/null -w '%{{http_code}}' "
            f"{method_flag} "
            "https://music.vm.test:18443/healthz)\" = 204"
        )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "'https://music.vm.test:18443/healthz?probe=1')\" = 404"
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "https://music.vm.test:18443/healthz/)\" = 401"
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' -X POST "
        "https://music.vm.test:18443/healthz)\" = 403"
    )

    # The exact-host vhost wins only for the canonical public Host. Attacker
    # and IP-literal Host values hit the default connection-closing vhost,
    # including on the anonymous health path.
    for supplied_host in ("attacker.invalid", "127.0.0.1"):
        machine.succeed(
            f"test \"$(curl -s -o /dev/null -w '%{{http_code}}' "
            f"-H 'Host: {supplied_host}' "
            "https://music.vm.test:18443/healthz || true)\" = 000"
        )

    # Gateway responses carry the frame/resource isolation policy on
    # documents, static resources, API responses, and an audio-route error.
    for index, path in enumerate((
        "/",
        "/js/main.js",
        "/api/_index",
        "/api/wrong-matches/audio",
    )):
        headers = f"/tmp/cratedigger-web-headers-{index}"
        machine.succeed(
            f"curl -sS -D {headers} -o /dev/null "
            "--user test-operator:test-password "
            f"https://music.vm.test:18443{path}"
        )
        raw_headers = machine.succeed(f"cat {headers}")
        _assert_exact_response_header(
            raw_headers,
            "Content-Security-Policy",
            "frame-ancestors 'none'",
        )
        _assert_exact_response_header(raw_headers, "X-Frame-Options", "DENY")
        _assert_exact_response_header(
            raw_headers,
            "Cross-Origin-Resource-Policy",
            "same-origin",
        )
        for cors_name in (
            "Access-Control-Allow-Origin",
            "Access-Control-Allow-Credentials",
            "Access-Control-Allow-Methods",
            "Access-Control-Allow-Headers",
        ):
            _assert_absent_response_header(raw_headers, cors_name)

    def _assert_basic_auth_matrix():
        machine.succeed(
            "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
            "https://music.vm.test:18443/)\" = 401"
        )
        machine.succeed(
            "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
            "--user test-operator:wrong "
            "https://music.vm.test:18443/)\" = 401"
        )
        machine.succeed(
            "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
            "--user test-operator:test-password "
            "https://music.vm.test:18443/)\" = 200"
        )

    def _gateway_markers():
        output = machine.succeed(
            "find /run/cratedigger-web -maxdepth 1 -type f "
            "-name 'gateway-*' -printf '%p\\n' | sort"
        )
        return [line for line in output.splitlines() if line]

    def _assert_gateway_marker(active):
        markers = _gateway_markers()
        if active:
            assert len(markers) == 1, markers
            marker = markers[0]
            assert re.fullmatch(
                r"/run/cratedigger-web/gateway-policy-[0-9a-f]{64}",
                marker,
            ), marker
            machine.succeed(
                f"test \"$(stat -c %U:%G:%a {marker})\" "
                "= root:cratedigger-web:440"
            )
            return marker
        else:
            assert markers == [], markers
            return None

    def _assert_cratedigger_fail_closed(label):
        _assert_gateway_marker(False)
        machine.succeed("systemctl is-active --quiet nginx.service")
        for index, url in enumerate((
            "https://music.vm.test:18443/",
            "https://music.vm.test:18443/healthz",
            "http://127.0.0.1:18086/",
            "http://127.0.0.1:18086/healthz",
        )):
            headers = f"/tmp/fail-closed-{index}.headers"
            status = machine.succeed(
                f"curl --max-time 5 -sS -D {headers} -o /dev/null "
                "-H 'Host: music.vm.test' "
                f"-w '%{{http_code}}' {url}"
            ).strip()
            if status != "503":
                _print_gateway_diagnostics(label)
            assert status == "503", (label, url, status)
            raw_headers = machine.succeed(f"cat {headers}")
            _assert_absent_response_header(raw_headers, "Location")
        machine.succeed(
            "test \"$(curl --max-time 5 -sS -o /dev/null "
            "-w '%{http_code}' -H 'Host: unrelated.vm.test' "
            "http://127.0.0.1:18087/)\" = 204"
        )

    def _reload_nginx(expect_success, label):
        status, output = machine.execute(
            "${pkgs.coreutils}/bin/timeout 30s "
            "systemctl reload nginx.service"
        )
        succeeded = status == 0
        if succeeded != expect_success:
            print(output)
            _print_gateway_diagnostics(label)
        assert succeeded == expect_success, (label, status, output)

    def _service_runtime_identity(unit):
        invocation = machine.succeed(
            f"systemctl show {unit} --property=InvocationID --value"
        ).strip()
        main_pid = machine.succeed(
            f"systemctl show {unit} --property=MainPID --value"
        ).strip()
        assert re.fullmatch(r"[0-9a-f]{32}", invocation), (
            unit, invocation,
        )
        assert int(main_pid) > 1, (unit, main_pid)
        return invocation, main_pid

    def _unit_invocation(unit):
        invocation = machine.succeed(
            f"systemctl show {unit} --property=InvocationID --value"
        ).strip()
        assert re.fullmatch(r"[0-9a-f]{32}", invocation), (
            unit, invocation,
        )
        return invocation

    def _nginx_worker_pids():
        main_pid = _service_runtime_identity("nginx.service")[1]
        workers = machine.succeed(
            f"ps --ppid {main_pid} -o pid= | sort -n"
        ).split()
        assert workers, main_pid
        return tuple(workers)

    def _wait_for_nginx_workers_changed(previous):
        previous_text = "\n".join(previous)
        machine.wait_until_succeeds(
            "main=$(systemctl show nginx.service "
            "--property=MainPID --value); "
            "current=$(ps --ppid \"$main\" -o pid= | sort -n); "
            "test -n \"$current\"; "
            f"previous={shlex.quote(previous_text)}; "
            "test \"$current\" != \"$previous\"",
            timeout=10,
        )

    # Fault-qualify the effective-identity ExecStartPre on the real shared
    # nginx unit. A downstream runtime override adds numeric GID 0 without
    # changing the configured services.nginx worker name; the unprivileged
    # preflight must reject it before gateway readiness is published. Removing
    # the drop-in restores the ordinary NixOS service, unrelated virtual host,
    # and reload-only worker replacement behavior.
    identity_fault_web = _service_runtime_identity(
        "cratedigger-web.service"
    )
    machine.succeed("systemctl stop nginx.service")
    machine.succeed(
        "install -d -m 0755 /run/systemd/system/nginx.service.d; "
        "printf '%s\\n' "
        "'[Service]' "
        "'SupplementaryGroups=' "
        "'SupplementaryGroups=0' "
        "'Restart=no' "
        "> /run/systemd/system/nginx.service.d/"
        "cratedigger-forbidden-identity.conf; "
        "systemctl daemon-reload"
    )
    forbidden_status, forbidden_output = machine.execute(
        "${pkgs.coreutils}/bin/timeout 30s "
        "systemctl start nginx.service"
    )
    if forbidden_status == 0:
        print(forbidden_output)
        _print_gateway_diagnostics("forbidden nginx effective identity")
    assert forbidden_status != 0, forbidden_output
    machine.succeed("systemctl is-failed --quiet nginx.service")
    machine.fail(
        "find /run/cratedigger-web -maxdepth 1 -type f "
        "-name 'gateway-policy-*' -print -quit | grep -q ."
    )
    machine.succeed(
        "journalctl -b -u nginx.service --no-pager "
        "| grep -F 'effective nginx group set contains forbidden root GID 0'"
    )
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == identity_fault_web
    machine.succeed(
        "rm /run/systemd/system/nginx.service.d/"
        "cratedigger-forbidden-identity.conf; "
        "systemctl daemon-reload; "
        "systemctl reset-failed nginx.service; "
        "systemctl start nginx.service"
    )
    machine.wait_for_unit("nginx.service")
    machine.wait_for_open_port(18086)
    machine.succeed(
        "test \"$(systemctl show nginx.service "
        "--property=Restart --value)\" = always"
    )
    _assert_basic_auth_matrix()
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        "-w '%{http_code}' -H 'Host: unrelated.vm.test' "
        "http://127.0.0.1:18087/)\" = 204"
    )
    restored_nginx_identity = _service_runtime_identity("nginx.service")
    restored_workers = _nginx_worker_pids()
    _reload_nginx(True, "reload after forbidden nginx identity restoration")
    assert _service_runtime_identity(
        "nginx.service"
    ) == restored_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == identity_fault_web
    _wait_for_nginx_workers_changed(restored_workers)
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        "-w '%{http_code}' -H 'Host: unrelated.vm.test' "
        "http://127.0.0.1:18087/)\" = 204"
    )

    # The empty-file branch fails a real reload closed, then a restored
    # credential can publish readiness only after nginx has accepted the new
    # policy.
    machine.succeed(
        "cp -a /run/cratedigger-test-auth/basic.htpasswd "
        "/run/cratedigger-test-auth/basic.htpasswd.good"
    )
    invalid_nginx_identity = _service_runtime_identity("nginx.service")
    invalid_web_identity = _service_runtime_identity(
        "cratedigger-web.service"
    )
    invalid_workers = _nginx_worker_pids()
    machine.succeed(
        "install -o root -g nginx -m 0440 /dev/null "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(False, "empty Basic credential reload")
    assert _service_runtime_identity(
        "nginx.service"
    ) == invalid_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == invalid_web_identity
    assert _nginx_worker_pids() == invalid_workers
    machine.succeed(
        "test \"$(systemctl show nginx.service "
        "--property=ReloadResult --value)\" = exit-code"
    )
    _assert_cratedigger_fail_closed("empty Basic credential reload")
    machine.succeed(
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic.htpasswd.good "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(True, "restore Basic after empty credential")
    assert _service_runtime_identity(
        "nginx.service"
    ) == invalid_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == invalid_web_identity
    _wait_for_nginx_workers_changed(invalid_workers)
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()

    # A same-path sops-style replacement reloads nginx without restarting the
    # shared master or the application. The HUP replaces only nginx workers;
    # the displaced password is denied immediately.
    machine.succeed(
        "${pkgs.apacheHttpd}/bin/htpasswd -bcB -C 4 "
        "/run/cratedigger-test-auth/basic-rotated.htpasswd "
        "test-operator rotated-password; "
        "chown root:nginx "
        "/run/cratedigger-test-auth/basic-rotated.htpasswd; "
        "chmod 0440 "
        "/run/cratedigger-test-auth/basic-rotated.htpasswd"
    )
    rotated_nginx_identity = _service_runtime_identity("nginx.service")
    rotated_web_identity = _service_runtime_identity(
        "cratedigger-web.service"
    )
    rotated_workers = _nginx_worker_pids()
    machine.succeed(
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic-rotated.htpasswd "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(True, "same-path Basic credential rotation")
    assert _service_runtime_identity(
        "nginx.service"
    ) == rotated_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == rotated_web_identity
    _wait_for_nginx_workers_changed(rotated_workers)
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "https://music.vm.test:18443/)\" = 401; "
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:rotated-password "
        "https://music.vm.test:18443/)\" = 200; "
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "-H 'Host: unrelated.vm.test' "
        "http://127.0.0.1:18087/)\" = 204"
    )
    restore_workers = _nginx_worker_pids()
    machine.succeed(
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic.htpasswd.good "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(True, "restore original Basic credential")
    assert _service_runtime_identity(
        "nginx.service"
    ) == rotated_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == rotated_web_identity
    _wait_for_nginx_workers_changed(restore_workers)
    _assert_basic_auth_matrix()
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:rotated-password "
        "https://music.vm.test:18443/)\" = 401"
    )

    # Test-only header recorder: temporarily replace only ExecStart while
    # retaining the production service identity, groups, sandbox, socket, and
    # nginx gateway. This proves the runtime header boundary without adding a
    # production introspection route.
    machine.succeed("systemctl stop cratedigger-web.service")
    machine.succeed(
        "install -d -m 0755 "
        "/run/systemd/system/cratedigger-web.service.d"
    )
    machine.succeed(
        "printf '%s\\n' '[Service]' 'ExecStart=' "
        "'ExecStart=${pkgs.python3}/bin/python3 ${headerRecorder}' "
        "> /run/systemd/system/cratedigger-web.service.d/header-recorder.conf"
    )
    machine.succeed(
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl"
    )
    machine.succeed("systemctl daemon-reload")
    machine.succeed("systemctl start cratedigger-web.service")
    machine.wait_until_succeeds(
        "systemctl is-active --quiet cratedigger-web.service"
    )
    machine.succeed(
        "test \"$(systemctl show cratedigger-web.service -p User --value)\" "
        "= cratedigger"
    )
    machine.succeed(
        "test \"$(systemctl show cratedigger-web.service -p Group --value)\" "
        "= cratedigger"
    )
    machine.succeed(
        "systemctl show cratedigger-web.service "
        "-p SupplementaryGroups --value "
        "| tr ' ' '\\n' | grep -Fx cratedigger-web"
    )
    machine.succeed(
        "test \"$(systemctl show cratedigger-web.service "
        "-p ProtectSystem --value)\" = strict"
    )
    machine.succeed(
        "test \"$(systemctl show cratedigger-web.service "
        "-p NoNewPrivileges --value)\" = yes"
    )
    machine.succeed(
        "test \"$(stat -c %U:%G:%a /run/cratedigger-web/web.sock)\" "
        "= root:cratedigger-web:660"
    )
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "test \"$(awk '/^Uid:/ {print $2}' /proc/$pid/status)\" "
        "= \"$(id -u cratedigger)\"; "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -F '${headerRecorder}'"
    )

    machine.succeed(
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl"
    )

    # Fault-qualify the anonymous health boundary with a backend that
    # deliberately leaves the declared body unread. The embedded CLI-channel
    # request must never become a second upstream request.
    smuggled_health_body = (
        b"POST /recorder/smuggled HTTP/1.1\r\n"
        b"Host: music.vm.test\r\n"
        b"X-Cratedigger-Request-Channel: cli\r\n"
        b"Content-Length: 0\r\n"
        b"Connection: close\r\n\r\n"
    )
    smuggled_health_request = (
        b"GET /healthz HTTP/1.1\r\n"
        b"Host: music.vm.test\r\n"
        + f"Content-Length: {len(smuggled_health_body)}\r\n".encode()
        + b"Connection: close\r\n\r\n"
        + smuggled_health_body
    )
    health_status, _health_response = _raw_public(smuggled_health_request)
    assert health_status == 204, health_status
    machine.wait_until_succeeds(
        "test \"$(wc -l < "
        "/var/lib/cratedigger/test-header-recorder.jsonl)\" = 1"
    )
    smuggled_health_rows = machine.succeed(
        "cat /var/lib/cratedigger/test-header-recorder.jsonl"
    ).splitlines()
    assert len(smuggled_health_rows) == 1, smuggled_health_rows
    assert json.loads(smuggled_health_rows[0])["path"] == "/healthz"
    machine.succeed(
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl"
    )

    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'Host: music.vm.test' "
        "-H 'Content-Type: application/json' "
        "-H 'Accept: application/json' "
        "-H 'Range: bytes=0-7' "
        "-H 'Origin: https://music.vm.test' "
        "-H 'Referer: https://music.vm.test/recorder' "
        "-H 'Cookie: session=secret-cookie' "
        "-H 'Proxy-Authorization: Bearer proxy-secret' "
        "-H 'X-Bearer-Token: bearer-secret' "
        "-H 'X-Api-Token: token-secret' "
        "-H 'X-Auth-Request-User: spoofed-user' "
        "-H 'X-Auth-Request-Email: spoofed@example.invalid' "
        "-H 'X-Forwarded-User: forwarded-user' "
        "-H 'X-Forwarded-Email: forwarded@example.invalid' "
        "-H 'X-Forwarded-Host: attacker.invalid' "
        "-H 'Forwarded: for=192.0.2.99;host=attacker.invalid' "
        "-H 'X-Identity: administrator' "
        "-H 'X-Role: administrator' "
        "-H 'X-Groups: administrators' "
        "-H 'Connection: keep-alive, X-Smuggled' "
        "-H 'X-Smuggled: hop-by-hop-sentinel' "
        "-H 'X-Cratedigger-Request-Channel: cli' "
        "--data-binary '{\"x\":1}' "
        "https://music.vm.test:18443/recorder/probe?view=raw)\" = 200"
    )
    machine.wait_until_succeeds(
        "test \"$(wc -l < "
        "/var/lib/cratedigger/test-header-recorder.jsonl)\" = 1"
    )
    recorder_lines = machine.succeed(
        "cat /var/lib/cratedigger/test-header-recorder.jsonl"
    ).splitlines()
    assert len(recorder_lines) == 1, recorder_lines
    recorder_row = json.loads(recorder_lines[0])
    assert recorder_row["method"] == "POST", recorder_row
    assert recorder_row["path"] == "/recorder/probe?view=raw", recorder_row
    expected_backend_headers = {
        "accept": ["application/json"],
        "content-length": ["7"],
        "content-type": ["application/json"],
        "host": ["music.vm.test"],
        "origin": ["https://music.vm.test"],
        "range": ["bytes=0-7"],
        "referer": ["https://music.vm.test/recorder"],
        "x-cratedigger-request-channel": ["browser"],
    }
    assert recorder_row["headers"] == expected_backend_headers, recorder_row
    raw_backend_headers = recorder_row["raw_headers"]
    assert len(raw_backend_headers) == len(expected_backend_headers), recorder_row
    banned_backend_headers = {
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-bearer-token",
        "x-api-token",
        "x-auth-request-user",
        "x-auth-request-email",
        "x-forwarded-user",
        "x-forwarded-email",
        "x-forwarded-host",
        "forwarded",
        "x-identity",
        "x-role",
        "x-groups",
        "connection",
        "x-smuggled",
        "user-agent",
    }
    assert banned_backend_headers.isdisjoint(
        recorder_row["headers"],
    ), recorder_row

    def _assert_raw_gateway_rejection(request, expected_status):
        status, response = _raw_gateway(request)
        assert status == expected_status, (request, status, response)

    def _assert_raw_public_rejection(request, expected_status):
        status, response = _raw_public(request)
        assert status == expected_status, (request, status, response)

    basic_authorization = (
        b"Authorization: Basic "
        b"dGVzdC1vcGVyYXRvcjp0ZXN0LXBhc3N3b3Jk\r\n"
    )
    raw_framing_rejections = (
        (
            b"GET /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            b"Host: attacker.invalid\r\n"
            b"Connection: close\r\n\r\n",
            400,
        ),
        (
            b"GET /recorder/rejected HTTP/1.1\r\n"
            b"Connection: close\r\n\r\n",
            400,
        ),
        (
            b"POST /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Content-Type: application/json\r\n"
            b"Content-Length: 2\r\n"
            b"Content-Length: 3\r\n"
            b"Connection: close\r\n\r\n{}",
            400,
        ),
        (
            b"POST /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Content-Type: application/json\r\n"
            b"Content-Length: 2\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n"
            b"2\r\n{}\r\n0\r\n\r\n",
            400,
        ),
        (
            b"POST /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Content-Type: application/json\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"Connection: close\r\n\r\n"
            b"ZZ\r\n{}\r\n0\r\n\r\n",
            400,
        ),
        (
            b"POST /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Transfer-Encoding: gzip\r\n"
            b"Connection: close\r\n\r\n",
            501,
        ),
        (
            b"POST /recorder/rejected HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Content-Type: application/json\r\n"
            b"Content-Length: 2\r\n"
            b"Content-Length: 3\r\n"
            b"Connection: keep-alive\r\n\r\n{}"
            b"GET /recorder/pipelined-valid HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Connection: close\r\n\r\n",
            400,
        ),
    )
    for request, expected_status in raw_framing_rejections:
        _assert_raw_public_rejection(request, expected_status)

    # These request-target variants deliberately probe the module-owned gateway
    # directly. An ordinary outer reverse proxy may normalise the request target
    # while constructing its upstream request; the public exact-health contract
    # is separately exercised above through TLS.
    raw_health_rejections = (
        (b"GET /healthz?probe=1 HTTP/1.1\r\n", 404),
        (b"GET /healthz%3fprobe HTTP/1.1\r\n", 401),
        (b"GET /alpha/../healthz HTTP/1.1\r\n", 404),
        (b"GET /alpha/%2e%2e/healthz HTTP/1.1\r\n", 404),
        (b"GET //healthz HTTP/1.1\r\n", 404),
        (b"GET /healthz%2f HTTP/1.1\r\n", 401),
        (b"OPTIONS /healthz HTTP/1.1\r\n", 403),
        (
            b"GET http://music.vm.test/healthz HTTP/1.1\r\n",
            400,
        ),
        (
            b"GET  http://music.vm.test/healthz HTTP/1.1\r\n",
            400,
        ),
        (
            b"GET      http://music.vm.test/healthz HTTP/1.1\r\n",
            400,
        ),
    )
    for request_line, expected_status in raw_health_rejections:
        _assert_raw_gateway_rejection(
            request_line
            + b"Host: music.vm.test\r\nConnection: close\r\n\r\n",
            expected_status,
        )

    # Rejections owned by nginx must leave the recorder's dispatch count
    # unchanged: query/method liveness variants, wrong Host, and failed Basic.
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "-H 'Host: music.vm.test' "
        "'https://music.vm.test:18443/healthz?probe=1')\" = 404"
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' -X POST "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/healthz)\" = 403"
    )
    machine.succeed(
        "test \"$(curl -s -o /dev/null -w '%{http_code}' "
        "-H 'Host: attacker.invalid' "
        "https://music.vm.test:18443/recorder/rejected || true)\" = 000"
    )
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:wrong "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/recorder/rejected)\" = 401"
    )
    machine.succeed(
        "test \"$(wc -l < "
        "/var/lib/cratedigger/test-header-recorder.jsonl)\" = 1"
    )

    # Remove the test-only ExecStart override, restore the ordinary application
    # behind the same still-owned socket, and prove authenticated app traffic.
    machine.succeed("systemctl stop cratedigger-web.service")
    machine.succeed(
        "rm -r /run/systemd/system/cratedigger-web.service.d"
    )
    machine.succeed("systemctl daemon-reload")
    machine.succeed("systemctl start cratedigger-web.service")
    machine.wait_until_succeeds(
        "systemctl is-active --quiet cratedigger-web.service"
    )
    # Type=simple becomes active before its shell wrapper has exec'd the
    # production server, so wait for the exact restored process identity.
    machine.wait_until_succeeds(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline | grep -F '${headerRecorder}'; "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -A1 -Fx -- '--canonical-origin' "
        "| tail -n1 | grep -Fx 'https://music.vm.test'",
        timeout=10,
    )
    _assert_basic_auth_matrix()
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/api/_index)\" = 200"
    )

    # Prove the production Unix-socket server itself can make progress while
    # another connection is blocked mid-header. The authorized local operator
    # holds an incomplete request open; the extra production worker must become
    # observable before an independent authenticated gateway request is given
    # a deliberately short deadline. A serial server times out here.
    machine.succeed(
        "rm -f "
        "/tmp/cratedigger-production-web-concurrency.blocked "
        "/tmp/cratedigger-production-web-concurrency.release "
        "/tmp/cratedigger-production-web-concurrency.threads; "
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "ps -o nlwp= -p \"$pid\" | tr -d ' ' "
        "> /tmp/cratedigger-production-web-concurrency.threads"
    )
    machine.succeed(
        "systemd-run --quiet "
        "--unit=cratedigger-production-web-concurrency "
        "--service-type=exec --uid=beets-operator "
        "--property=RuntimeMaxSec=15s "
        "${pkgs.python3}/bin/python3 ${productionWebConcurrencyProbe}"
    )
    machine.succeed(
        "${pkgs.coreutils}/bin/timeout 10s ${pkgs.bash}/bin/bash -c '"
        "baseline=$(cat "
        "/tmp/cratedigger-production-web-concurrency.threads); "
        "until test -f "
        "/tmp/cratedigger-production-web-concurrency.blocked && "
        "pid=$(systemctl show cratedigger-web.service "
        "-p MainPID --value) && "
        "current=$(ps -o nlwp= -p \"$pid\" | tr -d \" \") && "
        "test \"$current\" -gt \"$baseline\"; do "
        "systemctl is-active --quiet "
        "cratedigger-production-web-concurrency.service || exit 42; "
        "sleep 0.05; "
        "done' || { "
        "status=$?; "
        "systemctl status --no-pager "
        "cratedigger-production-web-concurrency.service "
        "cratedigger-web.service || true; "
        "journalctl -u cratedigger-production-web-concurrency.service "
        "-u cratedigger-web.service --no-pager -n 30 || true; "
        "exit \"$status\"; "
        "}"
    )
    machine.succeed(
        "systemctl is-active --quiet "
        "cratedigger-production-web-concurrency.service"
    )
    machine.succeed(
        "code=$(${pkgs.curl}/bin/curl --max-time 2 "
        "-sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/api/_index); "
        "status=$?; "
        "if test \"$status\" -ne 0 || test \"$code\" != 200; then "
        "touch /tmp/cratedigger-production-web-concurrency.release; "
        "echo \"parallel gateway status=$status code=$code\" >&2; "
        "systemctl status --no-pager "
        "cratedigger-production-web-concurrency.service "
        "cratedigger-web.service || true; "
        "journalctl -u cratedigger-production-web-concurrency.service "
        "-u cratedigger-web.service --no-pager -n 30 || true; "
        "exit 1; "
        "fi"
    )
    machine.succeed(
        "systemctl is-active --quiet "
        "cratedigger-production-web-concurrency.service"
    )
    machine.succeed(
        "touch /tmp/cratedigger-production-web-concurrency.release"
    )
    machine.succeed(
        "${pkgs.coreutils}/bin/timeout 15s ${pkgs.bash}/bin/bash -c '"
        "until state=$(systemctl show "
        "cratedigger-production-web-concurrency.service "
        "-p ActiveState --value) && "
        "{ test \"$state\" = inactive || test \"$state\" = failed; }; do "
        "sleep 0.05; "
        "done' || { "
        "status=$?; "
        "systemctl status --no-pager "
        "cratedigger-production-web-concurrency.service "
        "cratedigger-web.service || true; "
        "journalctl -u cratedigger-production-web-concurrency.service "
        "-u cratedigger-web.service --no-pager -n 30 || true; "
        "exit \"$status\"; "
        "}; "
        "test \"$(systemctl show "
        "cratedigger-production-web-concurrency.service "
        "-p ActiveState --value)\" = inactive && "
        "test \"$(systemctl show "
        "cratedigger-production-web-concurrency.service "
        "-p Result --value)\" = success || { "
        "status=$?; "
        "systemctl status --no-pager "
        "cratedigger-production-web-concurrency.service "
        "cratedigger-web.service || true; "
        "journalctl -u cratedigger-production-web-concurrency.service "
        "-u cratedigger-web.service --no-pager -n 30 || true; "
        "exit \"$status\"; "
        "}"
    )

    # Derive the whole anonymous route sweep from the application's own
    # authenticated route registry. Regex registrations are materialised with
    # values that match their declared production pattern; nothing is
    # hand-listed or silently skipped.
    route_rows = json.loads(machine.succeed(
        "curl -sS --user test-operator:test-password "
        "https://music.vm.test:18443/api/_index"
    ))
    assert len(route_rows) > 50, len(route_rows)

    def _materialize_registered_path(pattern):
        if not pattern.startswith("^"):
            return pattern
        path = pattern.removeprefix("^").removesuffix("$")
        path = path.replace(
            r"([a-f0-9-]{36}|\d+)",
            "999999",
        )
        path = path.replace(
            r"([a-f0-9-]{36})",
            "00000000-0000-0000-0000-000000000000",
        )
        path = path.replace(r"([a-f0-9-]+)", "deadbeef")
        path = path.replace(r"(\d+)", "999999")
        assert not re.search(r"[\^\$\(\)\[\]\\]", path), (pattern, path)
        return path

    swept_routes = set()
    for route_row in route_rows:
        method = route_row["method"]
        path = _materialize_registered_path(route_row["path"])
        if route_row["path"].startswith("^"):
            assert re.fullmatch(route_row["path"], path) is not None, (
                route_row, path,
            )
        else:
            assert path == route_row["path"], (route_row, path)
        swept_routes.add((method, route_row["path"], path))
        if method == "GET":
            command = (
                "curl -sS -o /dev/null -w '%{http_code}' "
                "-H 'Host: music.vm.test' "
                f"https://music.vm.test:18443{path}"
            )
        else:
            assert method == "POST", route_row
            command = (
                "curl -sS -o /dev/null -w '%{http_code}' -X POST "
                "-H 'Host: music.vm.test' "
                "-H 'Content-Type: application/json' -d '{}' "
                f"https://music.vm.test:18443{path}"
            )
        status = machine.succeed(command).strip()
        assert status == "401", (route_row, path, status)
    assert len(swept_routes) == len(route_rows), (
        len(swept_routes), len(route_rows),
    )

    # Liveness is the only anonymous exception and is a constant empty 204 for
    # both exact methods. Keep header and body captures separate so curl cannot
    # turn a HEAD response's headers into a false body assertion.
    for index, method in enumerate(("GET", "HEAD")):
        header_path = f"/tmp/health-exact-{index}.headers"
        body_path = f"/tmp/health-exact-{index}.body"
        status = machine.succeed(
            f"curl -sS -X {method} -D {header_path} -o {body_path} "
            "-w '%{http_code}' -H 'Host: music.vm.test' "
            "https://music.vm.test:18443/healthz"
        ).strip()
        assert status == "204", (method, status)
        machine.succeed(f"test ! -s {body_path}")
        raw_headers = machine.succeed(f"cat {header_path}")
        _assert_health_response_headers(raw_headers)

    # Same-origin provenance through real nginx. Valid Origin, Referer
    # fallback, and matching-both reach route validation (400 for the
    # deliberately incomplete body); every malformed/missing/mismatched world
    # is rejected by request security (403) with no pipeline state change.
    rows_before_provenance = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests'"
    ).strip()
    missing_delete_rows_before = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests WHERE id = 999999'"
    ).strip()
    valid_provenance_headers = (
        "-H 'Origin: https://music.vm.test'",
        "-H 'Referer: https://music.vm.test/some/page'",
        (
            "-H 'Origin: https://music.vm.test' "
            "-H 'Referer: https://music.vm.test/some/page'"
        ),
    )
    for provenance_headers in valid_provenance_headers:
        status = machine.succeed(
            "curl -sS -o /dev/null -w '%{http_code}' "
            "--user test-operator:test-password "
            "-H 'Host: music.vm.test' "
            "-H 'Content-Type: application/json' "
            f"{provenance_headers} -d '{{}}' "
            "https://music.vm.test:18443/api/pipeline/add"
        ).strip()
        assert status == "400", (provenance_headers, status)
    rejected_provenance_headers = (
        "",
        "-H 'Origin: null'",
        "-H 'Origin: https://attacker.invalid'",
        "-H 'Origin: https://music.vm.test,https://attacker.invalid'",
        (
            "-H 'Origin: https://music.vm.test' "
            "-H 'Referer: https://attacker.invalid/path'"
        ),
        "-H 'Referer: https://attacker.invalid/path'",
    )
    for provenance_headers in rejected_provenance_headers:
        status = machine.succeed(
            "curl -sS -o /dev/null -w '%{http_code}' "
            "--user test-operator:test-password "
            "-H 'Host: music.vm.test' "
            "-H 'Content-Type: application/json' "
            f"{provenance_headers} -d '{{}}' "
            "https://music.vm.test:18443/api/pipeline/add"
        ).strip()
        assert status == "403", (provenance_headers, status)

    duplicate_provenance_requests = (
        (
            b"Origin: https://music.vm.test\r\n"
            b"Origin: https://attacker.invalid\r\n"
        ),
        (
            b"Referer: https://music.vm.test/page\r\n"
            b"Referer: https://attacker.invalid/page\r\n"
        ),
    )
    for duplicate_headers in duplicate_provenance_requests:
        status, response = _raw_public(
            b"POST /api/pipeline/delete HTTP/1.1\r\n"
            b"Host: music.vm.test\r\n"
            + basic_authorization
            + b"Content-Type: application/json\r\n"
            b"Content-Length: 13\r\n"
            + duplicate_headers
            + b"Connection: close\r\n\r\n"
            b'{"id":999999}'
        )
        assert status == 403, (duplicate_headers, status, response)
        machine.succeed(
            "systemctl is-active --quiet cratedigger-web.service"
        )
    # The same canonical route remains healthy immediately afterward and
    # reaches its harmless not-found result with one valid provenance header.
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "-H 'Host: music.vm.test' "
        "-H 'Content-Type: application/json' "
        "-H 'Origin: https://music.vm.test' "
        "--data-binary '{\"id\":999999}' "
        "https://music.vm.test:18443/api/pipeline/delete)\" = 404"
    )
    machine.succeed("systemctl is-active --quiet cratedigger-web.service")
    rows_after_provenance = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests'"
    ).strip()
    assert rows_after_provenance == rows_before_provenance, (
        rows_before_provenance, rows_after_provenance,
    )
    missing_delete_rows_after = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests WHERE id = 999999'"
    ).strip()
    assert missing_delete_rows_after == missing_delete_rows_before == "0", (
        missing_delete_rows_before, missing_delete_rows_after,
    )

    # Every installed API-backed mutation reaches the canonical application
    # route over the fixed Unix socket as the authorized operator. Harmless
    # impossible identifiers produce the route's JSON 404/exit-2 contract;
    # upgrade uses whitespace so its route-local normalization returns the
    # canonical validation 400/exit-3 without a mirror call.
    api_cli_commands = (
        ("pipeline-delete 999999 --confirm DELETE", 2),
        ("set-quality not-a-real-release --status wanted", 2),
        ("upgrade ' '", 3),
        ("wrong-match-converge 999999 150 --apply", 2),
        ("resolve-rg 999999", 2),
    )
    for index, (command, expected_exit) in enumerate(api_cli_commands):
        stdout_path = f"/tmp/api-cli-{index}.stdout"
        stderr_path = f"/tmp/api-cli-{index}.stderr"
        machine.succeed(
            "set +e; "
            f"runuser -u beets-operator -- pipeline-cli {command} "
            f"> {stdout_path} 2> {stderr_path}; "
            f"rc=$?; set -e; test \"$rc\" = {expected_exit}; "
            f"test -s {stdout_path}; "
            f"! grep -Eq 'api_(unavailable|protocol_error)' "
            f"{stdout_path} {stderr_path}"
        )
        cli_payload = json.loads(machine.succeed(f"cat {stdout_path}"))
        assert isinstance(cli_payload, dict), (command, cli_payload)


    # Issue #1063: every command that touches a protected quarantine path
    # runs through the same socket. Impossible identifiers give the route's
    # own 404/exit-2 contract without any mirror, filesystem, or Beets work.
    # (command, expected exit, expects a JSON object on stdout). Every one
    # of these renders the ROUTE's answer to stdout; argparse's own usage
    # error also exits 2, also writes only to stderr, and also contains no
    # ``api_`` token, so an exit-code-only assertion would stay green while
    # nothing ever reached the socket.
    protected_path_cli_commands = (
        ("wrong-match-delete 999999 --apply --json", 2, True),
        ("force-import 999999", 2, False),
        (
            "beets-distance 999999 aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa --json",
            2,
            True,
        ),
        ("replace 999999 --to aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa --json", 2, True),
    )
    for index, (command, expected_exit, expects_json) in enumerate(
        protected_path_cli_commands,
    ):
        stdout_path = f"/tmp/protected-cli-{index}.stdout"
        stderr_path = f"/tmp/protected-cli-{index}.stderr"
        machine.succeed(
            "set +e; "
            f"runuser -u beets-operator -- pipeline-cli {command} "
            f"> {stdout_path} 2> {stderr_path}; "
            f"rc=$?; set -e; test \"$rc\" = {expected_exit}; "
            f"test -s {stdout_path}; "
            f"! grep -Eq 'api_(unavailable|protocol_error)' "
            f"{stdout_path} {stderr_path}"
        )
        protected_stdout = machine.succeed(f"cat {stdout_path}")
        if expects_json:
            protected_payload = json.loads(protected_stdout)
            assert isinstance(protected_payload, dict), (
                command, protected_payload,
            )
        else:
            # force-import has no --json flag; its rendered refusal is the
            # route's own message.
            assert "Force import rejected" in protected_stdout, (
                command, protected_stdout,
            )

    # The production ownership contract end to end: the private processing
    # tree is 0700 cratedigger, the operator cannot traverse it, and the
    # installed wrapper still deletes the exact folder through the socket
    # because the WEB SERVICE performs the deletion. Before issue #1063
    # this same command reported `deleted` while the folder survived.
    machine.succeed(
        "test \"$(stat -c %U:%a /var/lib/cratedigger/processing/albums)\" "
        "= cratedigger:700"
    )
    machine.succeed(
        "runuser -u cratedigger -- mkdir -p "
        "'/var/lib/cratedigger/processing/albums/wrong_matches/VM Artist - VM Album'"
    )
    machine.succeed(
        "runuser -u cratedigger -- sh -c \"printf audio > "
        "'/var/lib/cratedigger/processing/albums/wrong_matches/VM Artist - VM Album/01.mp3'\""
    )
    machine.fail(
        "runuser -u beets-operator -- test -r "
        "/var/lib/cratedigger/processing/albums"
    )
    wrong_match_request_id = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "INSERT INTO album_requests "
        "(mb_release_id, artist_name, album_title, source, status) VALUES "
        "('cccccccc-cccc-cccc-cccc-cccccccccccc', 'VM Artist', 'VM Album', "
        "'request', 'wanted') RETURNING id\""
    ).strip().splitlines()[0].strip()  # psql prints the command tag too
    wrong_match_log_id = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "INSERT INTO download_log (request_id, outcome, validation_result) "
        f"VALUES ({wrong_match_request_id}, 'rejected', "
        "'{\\\"scenario\\\": \\\"wrong_match\\\", \\\"failed_path\\\": "
        "\\\"/var/lib/cratedigger/processing/albums/wrong_matches/VM Artist - VM Album\\\"}'::jsonb) "
        "RETURNING id\""
    ).strip().splitlines()[0].strip()  # psql prints the command tag too
    machine.succeed(
        "set +e; "
        "runuser -u beets-operator -- pipeline-cli wrong-match-delete "
        f"{wrong_match_log_id} --apply --json "
        "> /tmp/protected-cli-delete.stdout "
        "2> /tmp/protected-cli-delete.stderr; "
        "rc=$?; set -e; test \"$rc\" = 0"
    )
    delete_payload = json.loads(
        machine.succeed("cat /tmp/protected-cli-delete.stdout")
    )
    assert delete_payload["outcome"] == "deleted", delete_payload
    assert delete_payload["path_missing"] is False, delete_payload
    assert delete_payload["deleted_path"] == (
        "/var/lib/cratedigger/processing/albums/wrong_matches/VM Artist - VM Album"
    ), delete_payload
    assert delete_payload["cleared_rows"] == 1, delete_payload
    machine.fail(
        "test -e '/var/lib/cratedigger/processing/albums/wrong_matches/"
        "VM Artist - VM Album'"
    )
    cleared_pointer = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "SELECT validation_result->>'failed_path' IS NULL FROM download_log "
        f"WHERE id = {wrong_match_log_id}\""
    ).strip()
    assert cleared_pointer == "t", cleared_pointer

    # An unobservable source is refused, not reported as missing: the same
    # command against a folder the SERVICE cannot read keeps both the folder
    # and its pointer, and exits 5 (retryable), never 0.
    machine.succeed(
        "runuser -u cratedigger -- mkdir -p "
        "'/var/lib/cratedigger/processing/albums/wrong_matches/VM Unreadable'"
    )
    unreadable_log_id = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "INSERT INTO download_log (request_id, outcome, validation_result) "
        f"VALUES ({wrong_match_request_id}, 'rejected', "
        "'{\\\"scenario\\\": \\\"wrong_match\\\", \\\"failed_path\\\": "
        "\\\"/var/lib/cratedigger/processing/albums/wrong_matches/VM Unreadable/Album\\\"}'::jsonb) "
        "RETURNING id\""
    ).strip().splitlines()[0].strip()  # psql prints the command tag too
    machine.succeed(
        "runuser -u cratedigger -- mkdir "
        "'/var/lib/cratedigger/processing/albums/wrong_matches/VM Unreadable/Album'"
    )
    machine.succeed(
        "chmod 000 '/var/lib/cratedigger/processing/albums/wrong_matches/"
        "VM Unreadable'"
    )
    machine.succeed(
        "set +e; "
        "runuser -u beets-operator -- pipeline-cli wrong-match-delete "
        f"{unreadable_log_id} --apply --json "
        "> /tmp/protected-cli-unreadable.stdout "
        "2> /tmp/protected-cli-unreadable.stderr; "
        "rc=$?; set -e; test \"$rc\" = 5"
    )
    unreadable_payload = json.loads(
        machine.succeed("cat /tmp/protected-cli-unreadable.stdout")
    )
    assert unreadable_payload["outcome"] == "skipped_path_unavailable", (
        unreadable_payload
    )
    assert unreadable_payload["path_missing"] is False, unreadable_payload
    assert unreadable_payload["deleted_path"] is None, unreadable_payload
    assert unreadable_payload["cleared_rows"] == 0, unreadable_payload
    machine.succeed(
        "chmod 700 '/var/lib/cratedigger/processing/albums/wrong_matches/"
        "VM Unreadable'"
    )
    machine.succeed(
        "test -d '/var/lib/cratedigger/processing/albums/wrong_matches/"
        "VM Unreadable/Album'"
    )
    retained_pointer = machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        "SELECT validation_result->>'failed_path' IS NOT NULL FROM download_log "
        f"WHERE id = {unreadable_log_id}\""
    ).strip()
    assert retained_pointer == "t", retained_pointer
    machine.succeed(
        "runuser -u cratedigger -- rm -rf "
        "'/var/lib/cratedigger/processing/albums/wrong_matches/VM Unreadable'"
    )
    machine.succeed(
        "sudo -u postgres psql cratedigger -At -c \""
        f"DELETE FROM album_requests WHERE id = {wrong_match_request_id}\""
    )

    # An unrelated user fails at Unix parent traversal. The structured
    # unavailable result cannot be a TCP or direct-DB fallback: the production
    # wrapper has selected the fixed socket and the Python process has no TCP
    # listener (both pinned above).
    machine.succeed(
        "set +e; "
        "runuser -u unrelated-user -- pipeline-cli upgrade "
        "not-a-real-release "
        "> /tmp/api-cli-unrelated.stdout "
        "2> /tmp/api-cli-unrelated.stderr; "
        "rc=$?; set -e; test \"$rc\" = 5; "
        "grep -q '\"error\": \"api_unavailable\"' "
        "/tmp/api-cli-unrelated.stderr; "
        "grep -Eqi 'permission denied|\\[Errno 13\\]' "
        "/tmp/api-cli-unrelated.stderr"
    )
    machine.fail("test -s /tmp/api-cli-unrelated.stdout")

    # Qualify socket lifecycle rather than only its steady state: stale node
    # replacement, direct service start activating its required socket, nginx
    # driving socket activation, representative restarts, and concurrent
    # threaded serving.
    machine.succeed(
        "systemctl stop nginx.service cratedigger-web.service "
        "cratedigger-web.socket"
    )
    machine.fail("test -e /run/cratedigger-web/web.sock")
    machine.succeed(
        "install -o root -g cratedigger-web -m 0660 /dev/null "
        "/run/cratedigger-web/web.sock"
    )
    machine.succeed("test -f /run/cratedigger-web/web.sock")
    machine.succeed("systemctl start cratedigger-web.service")
    machine.succeed("systemctl is-active --quiet cratedigger-web.socket")
    machine.succeed("systemctl is-active --quiet cratedigger-web.service")
    machine.succeed("test -S /run/cratedigger-web/web.sock")
    machine.succeed(
        "test \"$(stat -c %U:%G:%a /run/cratedigger-web/web.sock)\" "
        "= root:cratedigger-web:660"
    )
    machine.succeed("systemctl stop cratedigger-web.service")
    machine.succeed("systemctl start nginx.service")
    machine.wait_for_open_port(18086)
    machine.succeed(
        "test \"$(curl -sS -o /dev/null -w '%{http_code}' "
        "--user test-operator:test-password "
        "https://music.vm.test:18443/)\" = 200"
    )
    machine.succeed("systemctl is-active --quiet cratedigger-web.service")
    machine.succeed("systemctl restart cratedigger-web.service")
    _assert_basic_auth_matrix()
    machine.succeed("systemctl restart nginx.service")
    machine.wait_for_open_port(18086)
    _assert_basic_auth_matrix()
    machine.succeed(
        "seq 1 16 | xargs -P8 -I@ sh -c '"
        "test \"$(curl -sS -o /dev/null -w \"%{http_code}\" "
        "--user test-operator:test-password "
        "-H \"Host: music.vm.test\" "
        "https://music.vm.test:18443/api/_index)\" = 200'"
    )

    # Runtime-secret inspection, scoped honestly. The bcrypt hash is generated
    # after boot and is absent from generated nginx/unit configuration, the
    # application process, and its open descriptors. The TEST password itself
    # is intentionally present in the store-built fixture script; this is not
    # presented as a production plaintext-secret proof.
    machine.succeed(
        "hash=$(cut -d: -f2 /run/cratedigger-test-auth/basic.htpasswd); "
        "! grep -R -a -F -- \"$hash\" /etc/nginx /etc/systemd/system; "
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "! tr '\\0' '\\n' < /proc/$pid/environ | grep -F -- \"$hash\"; "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline | grep -F -- \"$hash\"; "
        "! find /proc/$pid/fd -maxdepth 1 -type l -exec readlink {} \\; "
        "| grep -F '/run/cratedigger-test-auth/basic.htpasswd'"
    )
    machine.succeed(
        "${pkgs.nginx}/bin/nginx -T -c /etc/nginx/nginx.conf "
        "> /tmp/nginx-generated-config 2>&1; "
        "grep -F 'auth_basic_user_file "
        "/run/cratedigger-test-auth/basic.htpasswd' "
        "/tmp/nginx-generated-config; "
        "hash=$(cut -d: -f2 /run/cratedigger-test-auth/basic.htpasswd); "
        "! grep -F -- \"$hash\" /tmp/nginx-generated-config"
    )
    machine.succeed(
        "nix-store -qR /run/current-system > /tmp/system-closure-paths; "
        "! grep -F '/run/cratedigger-test-auth/basic.htpasswd' "
        "/tmp/system-closure-paths; "
        "fixture=$(systemctl cat cratedigger-test-basic-auth.service "
        "| sed -n 's/^ExecStart=//p' | tail -n1); "
        "test -n \"$fixture\"; grep -F 'test-password' \"$fixture\""
    )

    # Exercise the real activation lifecycle, not just the Basic rendering
    # above: Basic -> explicit insecure -> alternate-path Basic -> Basic.
    # The specialisations change only the selected authentication policy; all
    # other perimeter invariants must survive every switch.
    insecure_warning = (
        "Authentication is disabled for this Cratedigger instance."
    )
    basic_system = machine.succeed(
        "readlink -f /run/current-system"
    ).strip()
    insecure_system = machine.succeed(
        "readlink -f "
        "/run/current-system/specialisation/cratedigger-insecure"
    ).strip()
    alternate_basic_system = machine.succeed(
        "readlink -f "
        "/run/current-system/specialisation/cratedigger-basic-alternate"
    ).strip()
    external_auth_system = machine.succeed(
        "readlink -f "
        "/run/current-system/specialisation/cratedigger-external-auth"
    ).strip()
    machine.succeed(f"test -x {basic_system}/bin/switch-to-configuration")
    machine.succeed(f"test -x {insecure_system}/bin/switch-to-configuration")
    machine.succeed(
        f"test -x {alternate_basic_system}/bin/switch-to-configuration"
    )
    machine.succeed(
        f"test -x {external_auth_system}/bin/switch-to-configuration"
    )

    def _print_web_transition_diagnostics(label):
        print(f"web transition diagnostics: {label}")
        print(machine.succeed(
            "systemctl status --no-pager nginx.service "
            "cratedigger-web.socket cratedigger-web.service || true"
        ))
        print(machine.succeed(
            "journalctl -u nginx.service -u cratedigger-web.service "
            "--no-pager -n 80 || true"
        ))
        print(machine.succeed(
            "${pkgs.nginx}/bin/nginx -T "
            "-c /etc/nginx/nginx.conf 2>&1 || true"
        ))

    # ``readiness_args`` carries whatever the FRONT PROXY requires of an
    # ordinary request. Under external authorization the operator's authorizer
    # covers /healthz like everything else, so the readiness probe must
    # authenticate; the module's own anonymous exception is asserted
    # separately, directly against its loopback gateway.
    def _switch_web_system(system_path, label, readiness_args=""):
        nginx_identity_before = _service_runtime_identity("nginx.service")
        reload_invocation_before = _unit_invocation(
            "nginx-config-reload.service"
        )
        status, output = machine.execute(
            f"${pkgs.coreutils}/bin/timeout 120s "
            f"{system_path}/bin/switch-to-configuration test"
        )
        if status != 0:
            print(output)
            _print_web_transition_diagnostics(label)
        assert status == 0, (
            f"{label} switch failed with status {status}: {output}"
        )
        ready_status, ready_output = machine.execute(
            "${pkgs.coreutils}/bin/timeout 30s "
            "${pkgs.bash}/bin/bash -c '"
            "until systemctl is-active --quiet nginx.service "
            "&& systemctl is-active --quiet cratedigger-web.socket "
            "&& test \"$(curl --max-time 2 -sS -o /dev/null "
            f"-w %{{http_code}} -H Host:music.vm.test {readiness_args} "
            "https://music.vm.test:18443/healthz)\" = 204 "
            "&& systemctl is-active --quiet cratedigger-web.service; "
            "do sleep 0.1; done'"
        )
        if ready_status != 0:
            print(ready_output)
            _print_web_transition_diagnostics(label)
        assert ready_status == 0, (
            f"{label} web services did not become ready: {ready_output}"
        )
        assert _service_runtime_identity(
            "nginx.service"
        ) == nginx_identity_before, label
        reload_invocation_after = _unit_invocation(
            "nginx-config-reload.service"
        )
        assert reload_invocation_after != reload_invocation_before, (
            label,
            reload_invocation_before,
            reload_invocation_after,
        )
        _assert_gateway_marker(True)

    def _switch_web_system_expect_failure(system_path, label):
        nginx_identity_before = _service_runtime_identity("nginx.service")
        reload_invocation_before = _unit_invocation(
            "nginx-config-reload.service"
        )
        status, output = machine.execute(
            f"${pkgs.coreutils}/bin/timeout 120s "
            f"{system_path}/bin/switch-to-configuration test"
        )
        if status == 0:
            print(output)
            _print_web_transition_diagnostics(label)
        assert status != 0, f"{label} unexpectedly succeeded: {output}"
        assert _service_runtime_identity(
            "nginx.service"
        ) == nginx_identity_before, label
        reload_invocation_after = _unit_invocation(
            "nginx-config-reload.service"
        )
        assert reload_invocation_after != reload_invocation_before, (
            label,
            reload_invocation_before,
            reload_invocation_after,
        )
        machine.succeed(
            "test \"$(systemctl show nginx.service "
            "--property=ReloadResult --value)\" = exit-code"
        )
        _assert_cratedigger_fail_closed(label)

    def _web_invocation_log():
        invocation = machine.succeed(
            "systemctl show cratedigger-web.service "
            "--property=InvocationID --value"
        ).strip()
        assert re.fullmatch(r"[0-9a-f]{32}", invocation), invocation
        return machine.succeed(
            f"journalctl --invocation={invocation} "
            "--output=cat --no-pager"
        )

    def _assert_loopback_socket_boundary():
        machine.succeed(
            "actual=$(ss -H -ltn 'sport = :18086' "
            "| awk '{print $4}' | sort); "
            "expected=$(printf '%s\\n' "
            "'127.0.0.1:18086' '127.0.0.2:18086' "
            "'[::1]:18086' | sort); "
            "test \"$actual\" = \"$expected\""
        )
        machine.fail("ss -H -ltn 'sport = :8085' | grep -q .")
        machine.succeed(
            "pid=$(systemctl show cratedigger-web.service "
            "-p MainPID --value); "
            "test \"$pid\" -gt 1; "
            "! ss -H -ltnp | grep -E \"pid=$pid([,)])\""
        )
        machine.succeed(
            "test \"$(stat -c %U:%G:%a /run/cratedigger-web)\" "
            "= root:cratedigger-web:750; "
            "test \"$(stat -c %U:%G:%a "
            "/run/cratedigger-web/web.sock)\" "
            "= root:cratedigger-web:660"
        )
        for identity in ("nginx", "beets-operator"):
            machine.succeed(
                f"runuser -u {identity} -- curl --max-time 5 -sS "
                "--unix-socket /run/cratedigger-web/web.sock "
                "-o /dev/null -w '%{http_code}' "
                "http://cratedigger.internal/healthz | grep -Fx 204"
            )
        machine.fail(
            "runuser -u unrelated-user -- curl --max-time 5 -sS "
            "--unix-socket /run/cratedigger-web/web.sock "
            "http://cratedigger.internal/healthz"
        )

    def _assert_exact_insecure_health():
        for index, method in enumerate(("GET", "HEAD")):
            header_path = f"/tmp/insecure-health-{index}.headers"
            body_path = f"/tmp/insecure-health-{index}.body"
            status = machine.succeed(
                f"curl --max-time 5 -sS -X {method} "
                f"-D {header_path} -o {body_path} -w '%{{http_code}}' "
                "-H 'Host: music.vm.test' "
                "https://music.vm.test:18443/healthz"
            ).strip()
            assert status == "204", (method, status)
            machine.succeed(f"test ! -s {body_path}")
            raw_headers = machine.succeed(f"cat {header_path}")
            _assert_health_response_headers(raw_headers)
        machine.succeed(
            "test \"$(curl --max-time 5 -sS -o /dev/null "
            "-w '%{http_code}' -H 'Host: music.vm.test' "
            "'https://music.vm.test:18443/healthz?probe=1')\" = 404"
        )
        machine.succeed(
            "test \"$(curl --max-time 5 -sS -o /dev/null "
            "-w '%{http_code}' -H 'Host: music.vm.test' "
            "https://music.vm.test:18443/healthz/)\" = 404"
        )
        machine.succeed(
            "test \"$(curl --max-time 5 -sS -o /dev/null "
            "-w '%{http_code}' -X POST -H 'Host: music.vm.test' "
            "https://music.vm.test:18443/healthz)\" = 403"
        )

    def _assert_resource_headers(credential_args):
        for index, path in enumerate((
            "/",
            "/js/main.js",
            "/api/_index",
            "/api/wrong-matches/audio",
        )):
            headers_path = f"/tmp/insecure-resource-{index}.headers"
            machine.succeed(
                f"curl --max-time 5 -sS -D {headers_path} "
                f"-o /dev/null {credential_args} "
                "-H 'Host: music.vm.test' "
                f"https://music.vm.test:18443{path}"
            )
            raw_headers = machine.succeed(f"cat {headers_path}")
            _assert_exact_response_header(
                raw_headers,
                "Content-Security-Policy",
                "frame-ancestors 'none'",
            )
            _assert_exact_response_header(
                raw_headers, "X-Frame-Options", "DENY",
            )
            _assert_exact_response_header(
                raw_headers,
                "Cross-Origin-Resource-Policy",
                "same-origin",
            )
            for cors_name in (
                "Access-Control-Allow-Origin",
                "Access-Control-Allow-Credentials",
                "Access-Control-Allow-Methods",
                "Access-Control-Allow-Headers",
            ):
                _assert_absent_response_header(raw_headers, cors_name)

    # Pin the starting side of the transition explicitly. Basic challenges are
    # the only mode behavior here; the insecure flag, warning, and footer are
    # absent.
    _assert_basic_auth_matrix()
    basic_policy_marker = _assert_gateway_marker(True)
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--insecure-mode'"
    )
    basic_body = machine.succeed(
        "curl --max-time 5 -sS --user test-operator:test-password "
        "https://music.vm.test:18443/"
    )
    assert basic_body.count(insecure_warning) == 0
    assert insecure_warning not in _web_invocation_log()

    _switch_web_system(insecure_system, "explicit insecure")
    machine.succeed(
        f"test \"$(readlink -f /run/current-system)\" = {insecure_system}"
    )
    insecure_policy_marker = _assert_gateway_marker(True)
    assert insecure_policy_marker != basic_policy_marker, (
        basic_policy_marker,
        insecure_policy_marker,
    )

    # Basic alone disappears: anonymous and deliberately wrong Basic
    # credentials both reach the app. The generated nginx config retains only
    # the exact health exception's explicit off-directive, with no challenge
    # or credential path.
    for credential_args in ("", "--user test-operator:wrong"):
        machine.succeed(
            "test \"$(curl --max-time 5 -sS -o /dev/null "
            f"-w '%{{http_code}}' {credential_args} "
            "-H 'Host: music.vm.test' "
            "https://music.vm.test:18443/)\" = 200"
        )
    insecure_nginx = machine.succeed(
        "${pkgs.nginx}/bin/nginx -T "
        "-c /etc/nginx/nginx.conf 2>&1"
    )
    assert "auth_basic_user_file" not in insecure_nginx, insecure_nginx
    insecure_basic_directives = [
        line.strip()
        for line in insecure_nginx.splitlines()
        if re.match(r"^[ \t]*auth_basic[ \t]+", line)
    ]
    assert insecure_basic_directives == ["auth_basic off;"], (
        insecure_basic_directives,
        insecure_nginx,
    )
    assert (
        "/run/cratedigger-test-auth/basic.htpasswd" not in insecure_nginx
    ), insecure_nginx
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--insecure-mode'; "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -A1 -Fx -- '--canonical-origin' "
        "| tail -n1 | grep -Fx 'https://music.vm.test'"
    )
    insecure_log = _web_invocation_log()
    warning_lines = [
        line for line in insecure_log.splitlines()
        if insecure_warning in line
    ]
    assert len(warning_lines) == 1, insecure_log
    assert re.search(
        r"\[CRITICAL\] "
        + re.escape(insecure_warning)
        + r"$",
        warning_lines[0],
    ), warning_lines
    insecure_body = machine.succeed(
        "curl --max-time 5 -sS -H 'Host: music.vm.test' "
        "https://music.vm.test:18443/"
    )
    assert insecure_body.count(insecure_warning) == 1, insecure_body
    assert insecure_body.count(
        '<footer class="insecure-auth-footer">'
    ) == 1, insecure_body

    # The canonical Host/origin envelope remains active without Basic. Valid
    # provenance reaches route-local validation; missing, mismatched, or
    # channel-spoofed provenance is rejected before dispatch. None of these
    # bounded probes can mutate the database.
    rows_before_insecure = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests'"
    ).strip()
    insecure_provenance_cases = (
        ("-H 'Origin: https://music.vm.test'", 400),
        ("", 403),
        ("-H 'Origin: https://attacker.invalid'", 403),
        ("-H 'X-Cratedigger-Request-Channel: cli'", 403),
    )
    for provenance_headers, expected_status in insecure_provenance_cases:
        status = machine.succeed(
            "curl --max-time 5 -sS -o /dev/null -w '%{http_code}' "
            "-H 'Host: music.vm.test' "
            "-H 'Content-Type: application/json' "
            f"{provenance_headers} -d '{{}}' "
            "https://music.vm.test:18443/api/pipeline/add"
        ).strip()
        assert status == str(expected_status), (
            provenance_headers, status,
        )
    rows_after_insecure = machine.succeed(
        "sudo -u postgres psql cratedigger -At "
        "-c 'SELECT count(*) FROM album_requests'"
    ).strip()
    assert rows_after_insecure == rows_before_insecure, (
        rows_before_insecure, rows_after_insecure,
    )
    machine.succeed(
        "test \"$(curl --max-time 5 -s -o /dev/null "
        "-w '%{http_code}' -H 'Host: attacker.invalid' "
        "https://music.vm.test:18443/healthz || true)\" = 000"
    )
    _assert_exact_insecure_health()
    _assert_loopback_socket_boundary()
    _assert_resource_headers("")

    # Reuse the real socket-activated service with a recorder only long enough
    # to observe the insecure gateway's backend request. The canonical Host
    # and browser channel are injected; identity, credential, forwarding, and
    # client-selected channel headers are stripped.
    machine.succeed("systemctl stop cratedigger-web.service")
    machine.succeed(
        "install -d -m 0755 "
        "/run/systemd/system/cratedigger-web.service.d"
    )
    machine.succeed(
        "printf '%s\\n' '[Service]' 'ExecStart=' "
        "'ExecStart=${pkgs.python3}/bin/python3 ${headerRecorder}' "
        "> /run/systemd/system/"
        "cratedigger-web.service.d/header-recorder.conf"
    )
    machine.succeed(
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl; "
        "systemctl daemon-reload; "
        "systemctl start cratedigger-web.service"
    )
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        "-w '%{http_code}' -H 'Host: music.vm.test' "
        "-H 'Accept: application/json' "
        "-H 'Range: bytes=0-7' "
        "-H 'Content-Type: application/json' "
        "-H 'Origin: https://music.vm.test' "
        "-H 'Referer: https://music.vm.test/transition' "
        "-H 'Authorization: Bearer attacker-secret' "
        "-H 'Cookie: session=attacker-secret' "
        "-H 'X-Forwarded-Host: attacker.invalid' "
        "-H 'X-Forwarded-User: attacker' "
        "-H 'X-Cratedigger-Request-Channel: cli' "
        "--data-binary '{\"x\":1}' "
        "https://music.vm.test:18443/transition/headers)\" = 200"
    )
    machine.succeed(
        "test \"$(wc -l < "
        "/var/lib/cratedigger/test-header-recorder.jsonl)\" = 1"
    )
    insecure_recorder_rows = machine.succeed(
        "cat /var/lib/cratedigger/test-header-recorder.jsonl"
    ).splitlines()
    assert len(insecure_recorder_rows) == 1, insecure_recorder_rows
    insecure_recorder_row = json.loads(insecure_recorder_rows[0])
    assert insecure_recorder_row["headers"] == {
        "accept": ["application/json"],
        "content-length": ["7"],
        "content-type": ["application/json"],
        "host": ["music.vm.test"],
        "origin": ["https://music.vm.test"],
        "range": ["bytes=0-7"],
        "referer": ["https://music.vm.test/transition"],
        "x-cratedigger-request-channel": ["browser"],
    }, insecure_recorder_row
    assert {
        "authorization",
        "cookie",
        "x-forwarded-host",
        "x-forwarded-user",
    }.isdisjoint(insecure_recorder_row["headers"]), insecure_recorder_row
    machine.succeed(
        "systemctl stop cratedigger-web.service; "
        "rm -r /run/systemd/system/cratedigger-web.service.d; "
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl; "
        "systemctl daemon-reload; "
        "systemctl start cratedigger-web.service"
    )
    machine.wait_until_succeeds(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--insecure-mode'",
        timeout=10,
    )

    # A failed insecure -> Basic deployment must not leave the old anonymous
    # nginx workers serving. Corrupt the runtime credential before activating
    # the Basic system: reload preparation removes readiness first, validation
    # aborts before policy-marker publication, and both public TLS and direct
    # gateway routes (including exact health) become a non-redirecting 503.
    # The unrelated vhost remains healthy in the same nginx process.
    machine.succeed("chmod 0644 /run/cratedigger-test-auth/basic.htpasswd")
    _switch_web_system_expect_failure(
        basic_system,
        "insecure to Basic with invalid credential",
    )

    # Explicit insecure recovery is still possible while the Basic credential
    # remains invalid because that selected mode does not consume the file.
    _switch_web_system(insecure_system, "recover explicit insecure")
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        "-w '%{http_code}' https://music.vm.test:18443/)\" = 200"
    )

    machine.succeed(
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic.htpasswd.good "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _switch_web_system(
        alternate_basic_system,
        "switch to alternate-path Basic",
    )
    machine.succeed(
        "test \"$(readlink -f /run/current-system)\" "
        f"= {alternate_basic_system}"
    )
    alternate_basic_policy_marker = _assert_gateway_marker(True)
    assert alternate_basic_policy_marker not in {
        basic_policy_marker,
        insecure_policy_marker,
    }, (
        basic_policy_marker,
        insecure_policy_marker,
        alternate_basic_policy_marker,
    )
    _assert_basic_auth_matrix()
    alternate_nginx = machine.succeed(
        "${pkgs.nginx}/bin/nginx -T "
        "-c /etc/nginx/nginx.conf 2>&1"
    )
    assert (
        "auth_basic_user_file "
        "/run/cratedigger-test-auth/basic-alternate.htpasswd;"
        in alternate_nginx
    ), alternate_nginx

    # ------------------------------------------------------------------
    # External authorization (issue #924).
    #
    # The composed world is the point: a real front proxy authorizes, the
    # real module gateway serves, and Cratedigger never contacts the
    # authorizer. Asserting only the module's own config would prove the
    # rendering and miss the composition.
    # ------------------------------------------------------------------
    _switch_web_system(
        external_auth_system,
        "switch to external authorization",
        "-H Cookie:vm_session=granted",
    )
    machine.succeed(
        "test \"$(readlink -f /run/current-system)\" "
        f"= {external_auth_system}"
    )
    external_policy_marker = _assert_gateway_marker(True)
    assert external_policy_marker not in {
        basic_policy_marker,
        insecure_policy_marker,
        alternate_basic_policy_marker,
    }, (
        basic_policy_marker,
        insecure_policy_marker,
        alternate_basic_policy_marker,
        external_policy_marker,
    )

    # Denied before Cratedigger. The authorizer refuses, so the request never
    # reaches the loopback gateway and the response is not the application's.
    external_denied_headers = "/tmp/external-denied.headers"
    external_denied_body = "/tmp/external-denied.body"
    denied_status = machine.succeed(
        f"curl --max-time 5 -sS -D {external_denied_headers} "
        f"-o {external_denied_body} -w '%{{http_code}}' "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/"
    ).strip()
    assert denied_status == "401", denied_status
    denied_body = machine.succeed(f"cat {external_denied_body}")
    assert "Music Pipeline" not in denied_body, denied_body
    assert insecure_warning not in denied_body, denied_body
    for denied_path in ("/api/pipeline/dashboard", "/js/main.js"):
        status = machine.succeed(
            "curl --max-time 5 -sS -o /dev/null -w '%{http_code}' "
            "-H 'Host: music.vm.test' "
            f"https://music.vm.test:18443{denied_path}"
        ).strip()
        assert status == "401", (denied_path, status)

    # Authorized requests reach the application unchanged.
    authorized = "-H 'Cookie: vm_session=granted'"
    external_body = machine.succeed(
        f"curl --max-time 5 -sS {authorized} "
        "-H 'Host: music.vm.test' https://music.vm.test:18443/"
    )
    assert "Music Pipeline" in external_body, external_body
    for authorized_path in ("/api/_index", "/api/pipeline/dashboard"):
        status = machine.succeed(
            "curl --max-time 5 -sS -o /dev/null -w '%{http_code}' "
            f"{authorized} -H 'Host: music.vm.test' "
            f"https://music.vm.test:18443{authorized_path}"
        ).strip()
        assert status == "200", (authorized_path, status)

    # The mode's whole purpose: no false claim that authentication is absent,
    # in the served document or in the journal.
    assert insecure_warning not in external_body, external_body
    assert '<footer class="insecure-auth-footer">' not in external_body, (
        external_body
    )
    external_log = _web_invocation_log()
    assert insecure_warning not in external_log, external_log
    assert "[CRITICAL]" not in external_log, external_log
    assert (
        "Browser authorization is owned by an external component in front of "
        "this Cratedigger gateway." in external_log
    ), external_log
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--external-auth-mode'; "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--insecure-mode'"
    )

    # No Basic residue: external mode never inherits or falls back to it.
    external_nginx = machine.succeed(
        "${pkgs.nginx}/bin/nginx -T -c /etc/nginx/nginx.conf 2>&1"
    )
    assert "auth_basic_user_file" not in external_nginx, external_nginx
    external_basic_directives = [
        line.strip()
        for line in external_nginx.splitlines()
        if re.match(r"^[ \t]*auth_basic[ \t]+", line)
    ]
    assert external_basic_directives == ["auth_basic off;"], (
        external_basic_directives,
    )

    # The module's anonymous health exception is unchanged at the gateway the
    # module owns, while the operator's own layer covers it on their terms —
    # here, denied like everything else. Both facts are the documented
    # deployment contract.
    assert machine.succeed(
        "curl --max-time 5 -sS -o /dev/null -w '%{http_code}' "
        "-H 'Host: music.vm.test' http://127.0.0.1:18086/healthz"
    ).strip() == "204"
    assert machine.succeed(
        "curl --max-time 5 -sS -o /dev/null -w '%{http_code}' "
        "-H 'Host: music.vm.test' "
        "https://music.vm.test:18443/healthz"
    ).strip() == "401"
    _assert_loopback_socket_boundary()
    _assert_resource_headers(authorized)

    # The authorizer's identity headers must not survive the gateway. This is
    # the composed proof: a real front proxy sets them, the real gateway
    # rebuilds its reviewed header set, and the backend sees neither.
    machine.succeed("systemctl stop cratedigger-web.service")
    machine.succeed(
        "rm -f /var/lib/cratedigger/test-header-recorder.jsonl; "
        "install -d -m 0755 "
        "/run/systemd/system/cratedigger-web.service.d; "
        "printf '%s\\n' '[Service]' 'ExecStart=' "
        "'ExecStart=${pkgs.python3}/bin/python3 ${headerRecorder}' "
        "> /run/systemd/system/"
        "cratedigger-web.service.d/header-recorder.conf; "
        "systemctl daemon-reload; "
        "systemctl start cratedigger-web.service"
    )
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        f"-w '%{{http_code}}' {authorized} "
        "-H 'Host: music.vm.test' "
        "-H 'Accept: application/json' "
        "https://music.vm.test:18443/external/headers)\" = 200"
    )
    external_recorder_rows = machine.succeed(
        "cat /var/lib/cratedigger/test-header-recorder.jsonl"
    ).splitlines()
    assert len(external_recorder_rows) == 1, external_recorder_rows
    external_recorder_row = json.loads(external_recorder_rows[0])
    assert {
        "remote-user",
        "remote-groups",
        "remote-email",
        "cookie",
        "authorization",
    }.isdisjoint(external_recorder_row["headers"]), external_recorder_row
    assert external_recorder_row["headers"]["host"] == ["music.vm.test"], (
        external_recorder_row
    )
    assert external_recorder_row["headers"][
        "x-cratedigger-request-channel"
    ] == ["browser"], external_recorder_row
    machine.succeed(
        "rm -f /run/systemd/system/"
        "cratedigger-web.service.d/header-recorder.conf; "
        "systemctl daemon-reload; "
        "systemctl restart cratedigger-web.service"
    )

    _switch_web_system(basic_system, "return to Basic")
    machine.succeed(
        f"test \"$(readlink -f /run/current-system)\" = {basic_system}"
    )
    restored_basic_policy_marker = _assert_gateway_marker(True)
    assert restored_basic_policy_marker == basic_policy_marker, (
        basic_policy_marker,
        restored_basic_policy_marker,
    )
    _assert_basic_auth_matrix()
    restored_nginx = machine.succeed(
        "${pkgs.nginx}/bin/nginx -T "
        "-c /etc/nginx/nginx.conf 2>&1"
    )
    assert (
        "auth_basic_user_file "
        "/run/cratedigger-test-auth/basic.htpasswd;"
        in restored_nginx
    ), restored_nginx
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -Fx -- '--insecure-mode'; "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -A1 -Fx -- '--canonical-origin' "
        "| tail -n1 | grep -Fx 'https://music.vm.test'"
    )
    restored_body = machine.succeed(
        "curl --max-time 5 -sS --user test-operator:test-password "
        "https://music.vm.test:18443/"
    )
    assert restored_body.count(insecure_warning) == 0, restored_body
    assert insecure_warning not in _web_invocation_log()
    _assert_loopback_socket_boundary()
    _assert_resource_headers("--user test-operator:test-password")

    # Fault-qualify the receipt boundary by replacing the trusted descriptor
    # after nginx has accepted/HUPed the config but before the finish hook.
    # The receipt is root-only, the byte mismatch prevents publication, and
    # restoring the exact activation-owned descriptor makes a later reload
    # recover without restarting nginx or the application.
    overlap_nginx_identity = _service_runtime_identity("nginx.service")
    overlap_web_identity = _service_runtime_identity(
        "cratedigger-web.service"
    )
    overlap_workers = _nginx_worker_pids()
    machine.succeed(
        "install -m 0600 /dev/null "
        "/run/cratedigger-test-mutate-policy-during-reload"
    )
    _reload_nginx(False, "descriptor changed during nginx reload")
    assert _service_runtime_identity(
        "nginx.service"
    ) == overlap_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == overlap_web_identity
    _wait_for_nginx_workers_changed(overlap_workers)
    machine.succeed(
        "test ! -e /run/cratedigger-web/gateway-reload-receipt"
    )
    _assert_cratedigger_fail_closed(
        "descriptor changed during nginx reload"
    )
    machine.succeed(
        "rm -f /etc/cratedigger/web-gateway-policy; "
        "test \"$(stat -c '%U:%G:%a' "
        "/run/cratedigger-test-policy-original)\" = root:root:600; "
        "install -o root -g root -m 0444 "
        "/run/cratedigger-test-policy-original "
        "/etc/cratedigger/web-gateway-policy; "
        "rm -f /run/cratedigger-test-policy-original"
    )
    overlap_recovery_workers = _nginx_worker_pids()
    _reload_nginx(True, "restore exact gateway policy descriptor")
    assert _service_runtime_identity(
        "nginx.service"
    ) == overlap_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == overlap_web_identity
    _wait_for_nginx_workers_changed(overlap_recovery_workers)
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()

    # Fault-qualify the credential fingerprint in the reload receipt
    # independently of descriptor identity. Replace the valid same-path
    # credential atomically after HUP but before the finish hook. The parsed
    # policy is unchanged, so only the receipt-bound credential hash can
    # prevent publication.
    credential_overlap_nginx_identity = _service_runtime_identity(
        "nginx.service"
    )
    credential_overlap_web_identity = _service_runtime_identity(
        "cratedigger-web.service"
    )
    credential_overlap_workers = _nginx_worker_pids()
    machine.succeed(
        "install -m 0600 /dev/null "
        "/run/cratedigger-test-mutate-credential-during-reload"
    )
    _reload_nginx(False, "credential changed during nginx reload")
    assert _service_runtime_identity(
        "nginx.service"
    ) == credential_overlap_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == credential_overlap_web_identity
    _wait_for_nginx_workers_changed(credential_overlap_workers)
    machine.succeed(
        "test ! -e /run/cratedigger-web/gateway-reload-receipt; "
        "cmp -s /run/cratedigger-test-auth/basic.htpasswd "
        "/run/cratedigger-test-auth/basic-rotated.htpasswd"
    )
    _assert_cratedigger_fail_closed(
        "credential changed during nginx reload"
    )
    machine.succeed(
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic.htpasswd.good "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    credential_recovery_workers = _nginx_worker_pids()
    _reload_nginx(True, "restore exact Basic credential after overlap")
    assert _service_runtime_identity(
        "nginx.service"
    ) == credential_overlap_nginx_identity
    assert _service_runtime_identity(
        "cratedigger-web.service"
    ) == credential_overlap_web_identity
    _wait_for_nginx_workers_changed(credential_recovery_workers)
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()

    # Exercise independent credential-validator branches through the real
    # nginx reload/start hooks. Each reload failure removes readiness only for
    # Cratedigger and is recoverable; the non-regular resolved-target branch
    # additionally proves a cold start cannot publish any listener or marker.
    machine.succeed(
        "${pkgs.acl}/bin/setfacl -m u:unrelated-user:--- "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(False, "extended Basic credential ACL")
    _assert_cratedigger_fail_closed("extended Basic credential ACL")
    machine.succeed(
        "${pkgs.acl}/bin/setfacl -b "
        "/run/cratedigger-test-auth/basic.htpasswd; "
        "chown root:nginx /run/cratedigger-test-auth/basic.htpasswd; "
        "chmod 0440 /run/cratedigger-test-auth/basic.htpasswd"
    )
    _reload_nginx(True, "restore Basic after extended ACL")
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()

    machine.succeed("chmod 0770 /run/cratedigger-test-auth")
    _reload_nginx(False, "group-writable Basic credential ancestor")
    _assert_cratedigger_fail_closed(
        "group-writable Basic credential ancestor"
    )
    machine.succeed("chmod 0750 /run/cratedigger-test-auth")
    _reload_nginx(True, "restore Basic after writable ancestor")
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()

    machine.succeed(
        "systemctl stop nginx.service; "
        "rm /run/cratedigger-test-auth/basic.htpasswd; "
        "install -d -o root -g nginx -m 0750 "
        "/run/cratedigger-test-auth/non-regular-target; "
        "ln -s /run/cratedigger-test-auth/non-regular-target "
        "/run/cratedigger-test-auth/basic.htpasswd"
    )
    cold_status, cold_output = machine.execute(
        "${pkgs.coreutils}/bin/timeout 30s systemctl start nginx.service"
    )
    if cold_status == 0:
        print(cold_output)
        _print_gateway_diagnostics("non-regular Basic target cold start")
    assert cold_status != 0, cold_output
    _assert_gateway_marker(False)
    machine.fail("ss -H -ltn 'sport = :18086' | grep -q .")
    machine.fail("ss -H -ltn 'sport = :18443' | grep -q .")
    machine.succeed(
        "rm /run/cratedigger-test-auth/basic.htpasswd; "
        "rmdir /run/cratedigger-test-auth/non-regular-target; "
        "install -o root -g nginx -m 0440 "
        "/run/cratedigger-test-auth/basic.htpasswd.good "
        "/run/cratedigger-test-auth/basic.htpasswd; "
        "systemctl reset-failed nginx.service; "
        "systemctl start nginx.service"
    )
    machine.wait_for_open_port(18086)
    machine.wait_for_open_port(18443)
    _assert_gateway_marker(True)
    _assert_basic_auth_matrix()
    machine.succeed(
        "test \"$(curl --max-time 5 -sS -o /dev/null "
        "-w '%{http_code}' -H 'Host: unrelated.vm.test' "
        "http://127.0.0.1:18087/)\" = 204"
    )

    # U13: cratedigger-unfindable.service + .timer exist and are
    # ordered correctly. Structural assertions only — we do NOT fire
    # the unit because slskd is not available in the VM. This guards
    # the module against future deployments that forget to render the
    # detection unit, or render it without the migrate dependency.
    machine.succeed("systemctl cat cratedigger-unfindable.service > /dev/null")
    machine.succeed("systemctl cat cratedigger-unfindable.timer > /dev/null")
    # After= must include the db-migrate unit so the detection job normally
    # runs behind a first-boot migration. Same deploy-kill-migrate fix as
    # cratedigger.service above: Wants=, NOT Requires= (restartIfChanged
    # here is false too, so a switch-time migrate restart must not
    # SIGTERM a mid-flight run) -- the fail-loud assert_schema_current
    # startup gate re-provides the "never runs against an un-migrated
    # schema" guarantee.
    machine.succeed("systemctl show -p After cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Wants cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    machine.fail("systemctl show -p Requires cratedigger-unfindable.service | grep -q cratedigger-db-migrate.service")
    # Timer is enabled (wantedBy timers.target) — the daily fire is
    # not opt-in. ``systemctl is-enabled`` returns "enabled" for units
    # wired into timers.target.
    enabled = machine.succeed("systemctl is-enabled cratedigger-unfindable.timer").strip()
    assert enabled == "enabled", f"unfindable timer not enabled: {enabled}"

    # U7: cratedigger-youtube-ingest.service. The worker is long-lived
    # (Type=simple); we verify it comes up active, idle (no pending
    # jobs in the test DB), and that the structural contracts hold:
    #
    #   - migrate-dependency ordering (Requires + After)
    #   - the wrapper exports `yt-dlp` onto the worker's PATH (worker-
    #     specific, NOT on the shared runtime path)
    #   - the per-process temp dir is created by systemd-tmpfiles
    #   - second-instance start exits 0 fast (advisory-lock contention)
    machine.wait_for_unit("cratedigger-youtube-ingest.service")
    state = machine.succeed("systemctl is-active cratedigger-youtube-ingest.service").strip()
    assert state == "active", f"youtube-ingest unit not active: {state}"

    machine.succeed("systemctl show -p After cratedigger-youtube-ingest.service | grep -q cratedigger-db-migrate.service")
    machine.succeed("systemctl show -p Requires cratedigger-youtube-ingest.service | grep -q cratedigger-db-migrate.service")

    # The wrapper exports yt-dlp's bin onto PATH for the worker process.
    # The wrapper binary itself is on systemPackages PATH; grep its body
    # for the yt-dlp path-prepend so we know the worker process's PATH
    # will resolve the binary.
    machine.succeed("grep -q 'yt-dlp.*bin' $(command -v cratedigger-youtube-ingest)")

    # The configured sourceAddress renders into the worker's ExecStart so
    # yt-dlp binds its client socket to the VPN-routed NIC (egress hardening).
    machine.succeed("grep -q -- '--source-address \"10.0.2.15\"' $(command -v cratedigger-youtube-ingest)")

    # The drainer's per-process temp dir was created by systemd-tmpfiles
    # with the same ownership as the cratedigger user.
    machine.succeed("test -d /var/lib/cratedigger/youtube-ingest-temp")

    # Advisory-lock contention: starting a second instance manually
    # must exit 0 (clean — duplicate-start is expected, not a crash)
    # and not respawn. The systemd unit holds the lock; this invocation
    # fails to acquire and returns 0 immediately.
    machine.succeed("sudo -u cratedigger cratedigger-youtube-ingest --once")

    # The deployment exposes plain `beet` from the supplied package and owns
    # its immutable config plus token-only include.  The retired
    # A Cratedigger operator wrapper and mutable application-owned BEETSDIR are absent.
    machine.fail("command -v cratedigger-beet")
    machine.succeed("command -v beet")
    machine.succeed("test -f ${externalBeetsConfigDir}/config.yaml")
    mode = machine.succeed(
        "stat -Lc %a ${externalBeetsConfigDir}/config.yaml"
    ).strip()
    assert mode == "444", f"external config.yaml should be 0444, got {mode}"
    secret_mode = machine.succeed(
        "stat -c %a ${externalSecretInclude}"
    ).strip()
    secret_group = machine.succeed(
        "stat -c %G ${externalSecretInclude}"
    ).strip()
    assert secret_mode == "440", f"token include should be 0440, got {secret_mode}"
    assert secret_group == "beets-library", secret_group
    machine.succeed(
        "sudo -u beets-operator test -r ${externalSecretInclude}"
    )
    machine.fail("sudo -u unrelated-user test -r ${externalSecretInclude}")

    version_out = machine.succeed(
        "sudo -u cratedigger env BEETSDIR=${externalBeetsConfigDir} beet version"
    )
    plugins_line = next(
        line for line in version_out.splitlines() if line.startswith("plugins:")
    )
    loaded = {p.strip() for p in plugins_line.split(":", 1)[1].split(",")}
    for plugin in (
        "musicbrainz mbsync discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    ).split():
        assert plugin in loaded, f"plugin {plugin} not loaded: {version_out}"
    operator_version = machine.succeed(
        "sudo -u beets-operator env BEETSDIR=${externalBeetsConfigDir} "
        "beet version"
    )
    operator_plugins = next(
        line for line in operator_version.splitlines() if line.startswith("plugins:")
    )
    assert operator_plugins == plugins_line, (operator_plugins, plugins_line)
    machine.succeed(
        "sudo -u beets-operator env BEETSDIR=${externalBeetsConfigDir} "
        "beet config > /dev/null"
    )
    service_groups = machine.succeed("id -nG cratedigger").split()
    assert "cratedigger-ops" not in service_groups, service_groups

    # Execute a real 12-track removal through the deployment-owned plain CLI,
    # then the explicit exact-album child, against the same supplied package.
    seed_out = machine.succeed(
        f"sudo -u cratedigger env BEETSDIR=${externalBeetsConfigDir} "
        f"{beets_python} ${beetsDestructiveFixture} seed"
    )
    child_album_id = int(seed_out.strip().split("=", 1)[1])
    remove_out = machine.succeed(
        "sudo -u beets-operator env BEETSDIR=${externalBeetsConfigDir} "
        "beet -P importsource "
        "remove -a -f -d mb_albumid:cccccccc-cccc-cccc-cccc-cccccccccccc"
    )
    assert "Really?" not in remove_out, remove_out
    child_request = json.dumps({
        "album_id": child_album_id,
        "expected_release_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "library_db_path": "/var/lib/cratedigger-beets-db/beets-library.db",
        "library_root": "/var/lib/cratedigger-music/Beets",
    }, separators=(",", ":"))
    child_out = machine.succeed(
        f"printf '%s' '{child_request}' | "
        f"sudo -u beets-operator env BEETSDIR=${externalBeetsConfigDir} "
        f"{beets_python} ${cratediggerSrc}/harness/delete_album.py "
        "2>/tmp/exact-delete.stderr"
    )
    child_payload = json.loads(child_out)
    assert child_payload["status"] == "completed", child_payload
    assert json.dumps(child_payload, separators=(",", ":")) == child_out
    machine.succeed("test ! -s /tmp/exact-delete.stderr")
    machine.succeed(
        f"sudo -u cratedigger env BEETSDIR=${externalBeetsConfigDir} "
        f"{beets_python} ${beetsDestructiveFixture} verify"
    )

    # Exercise intrinsic application enforcement through two alternate
    # deployment-owned authorities.  A hard conflict must stop before any
    # PipelineDB/Beets effect; warning-only drift must admit startup; restoring
    # the safe authority and restarting must converge without touching the
    # fixture library.
    def _beets_world_digest():
        return machine.succeed(
            "{ sha256sum ${externalLibraryDb} ${externalStateFile}; "
            "find ${externalLibraryRoot} -type f -print0 "
            "| sort -z | xargs -0 sha256sum; } "
            "| sha256sum | cut -d ' ' -f 1"
        ).strip()

    def _pipeline_data_snapshot():
        # #1204 review F6: fail closed, at the exact snapshot, if this
        # whole-DB equality window is ever entered while
        # cratedigger-unfindable.timer -- or the service it triggers -- is
        # live. Every call site below sits inside this phase's quiesced
        # region (the initial _quiesce_unfindable() call, a re-quiesce after
        # each of the three switch-to-configuration calls, and the
        # phase-end restart only after the very last such call) -- so this
        # never trips today. A future switch-to-configuration added inside
        # this phase without a matching _quiesce_unfindable() re-stop trips
        # this assertion here instead of producing an unattributable
        # pg_dump diff further down.
        #
        # Both units are checked (#1204 review round 3 item 6a).
        # cratedigger-unfindable.service carries no `wantedBy` of its own
        # (nix/module.nix -- confirmed against the tree: only the .timer has
        # `wantedBy = ["timers.target"]`), so switch-to-configuration's
        # unit-closure reconciliation can never start the SERVICE directly,
        # only the TIMER, which then triggers the service on its own
        # schedule. The timer check alone is therefore sufficient against
        # every writer this phase's reconciliation can produce today; the
        # service check closes that permanently rather than leaving the
        # no-wantedBy fact as an unstated assumption the guard's sufficiency
        # silently depends on -- a future unit definition that adds a
        # `wantedBy` to the service would otherwise reopen exactly the hole
        # this guard exists to close.
        unfindable_timer_status, _ = machine.execute(
            "systemctl is-active --quiet cratedigger-unfindable.timer"
        )
        assert unfindable_timer_status != 0, (
            "cratedigger-unfindable.timer is ACTIVE during a "
            "_pipeline_data_snapshot() call (#1204). This phase's quiesce "
            "contract requires the timer stopped before every whole-DB "
            "equality snapshot -- add a _quiesce_unfindable() call "
            "immediately after whatever switch-to-configuration or unit "
            "restart reactivated it."
        )
        unfindable_service_status, _ = machine.execute(
            "systemctl is-active --quiet cratedigger-unfindable.service"
        )
        assert unfindable_service_status != 0, (
            "cratedigger-unfindable.service is ACTIVE during a "
            "_pipeline_data_snapshot() call (#1204). This phase's quiesce "
            "contract requires the service stopped before every whole-DB "
            "equality snapshot -- add a _quiesce_unfindable() call "
            "immediately after whatever switch-to-configuration or unit "
            "restart reactivated it."
        )
        dump = machine.succeed(
            "runuser -u postgres -- pg_dump --data-only --no-owner "
            "cratedigger"
        )
        # PostgreSQL 18 emits a fresh random psql restriction key on every
        # dump. It is transport metadata, not database state.
        return "\n".join(
            line for line in dump.splitlines()
            if not line.startswith(("\\restrict ", "\\unrestrict "))
        )

    def _quiesce_unfindable():
        # switch-to-configuration reconciles the FULL unit closure declared
        # by the target generation on every call, including a specialisation
        # switch to an otherwise-identical generation: it (re)starts any unit
        # wanted by an active target -- multi-user.target's ordinary services
        # exactly as much as timers.target's timers -- that is not currently
        # active, regardless of this phase having stopped it moments earlier.
        # This is not timer-specific: three of the other four workers
        # (cratedigger-web, cratedigger-importer,
        # cratedigger-import-preview-worker) hit the same reconciliation and
        # are explicitly re-stopped later in this phase (see the repeated
        # `systemctl stop` calls for them, e.g. before the missing_state
        # scenario below) -- cratedigger-unfindable.timer was simply the one
        # nothing had been re-quiescing. cratedigger-youtube-ingest is the
        # exception (#1204 review round 3 item 5): switch1 restarts it too,
        # but nothing in this phase stops it again, so it runs across every
        # remaining equality window here -- see the PR body for why that
        # residual is benign. Confirmed empirically while developing this
        # fix: the very first switch-to-configuration call below restarted
        # cratedigger-unfindable.timer within 5 seconds of the initial stop,
        # well before that scenario's own equality assert. Call this
        # immediately after every switch-to-configuration call in this
        # phase.
        #
        # Ordering contract for every call site (#1204 review round 3 item
        # 6b -- moved here from the pipeline_before_hard call site so it
        # travels with this helper rather than living at one caller):
        # capture every `_pipeline_data_snapshot()` baseline in this phase
        # AFTER calling this function following whatever event reactivated
        # the timer, never before it. `_pipeline_data_snapshot()`'s own
        # guard only checks state at the instant of each snapshot call -- a
        # switch-to-configuration call that lands INSIDE an already-open
        # baseline-to-comparison window defeats even a correct re-quiesce
        # placed elsewhere in the phase, because nothing re-checks the guard
        # for the span between the two snapshots themselves.
        #
        # Stop the timer FIRST, in its own call, then the service in a
        # second call (#1204 review residual 1; corrected #1204 review round
        # 3 item 3). A `.timer` unit carries an implicit `Before=` on the
        # service it triggers (systemd.timer(5)); systemd inverts Before=/
        # After= ordering for stop jobs relative to start jobs, so a single
        # combined `systemctl stop timer service` invocation is GUARANTEED
        # to stop the SERVICE first and the TIMER second -- the exact wrong
        # order, since it leaves the timer alive (and able to re-trigger the
        # service) for the whole time its own stop job is still queued
        # behind the service's. This is not a "no guaranteed ordering" risk;
        # it is a guaranteed wrong one. Two separate calls, timer then
        # service, produce the order this phase actually needs: nothing can
        # re-trigger the service once the first call returns, and the
        # second call kills anything already in flight.
        machine.succeed("systemctl stop cratedigger-unfindable.timer")
        machine.succeed("systemctl stop cratedigger-unfindable.service")

    # #1204 defect 2: cratedigger-unfindable.timer is OnCalendar=daily +
    # Persistent=true + RandomizedDelaySec=30min (nix/module.nix). The real
    # arming mechanism is NOT this test's own clock excursion elsewhere in
    # this file (~line 2169, a net-zero backward-then-forward `date -s` used
    # for an unrelated stale-preview-snapshot scenario) -- it is the
    # ordinary boot-relative daily OnCalendar boundary plus Persistent=true
    # catch-up: every time the timer is (re)armed, if its persisted
    # last-trigger stamp shows the most recent daily boundary was missed,
    # systemd schedules a near-term catch-up elapse (still jittered by
    # RandomizedDelaySec) instead of waiting for tomorrow's boundary.
    #
    # The 2026-08-19 incident's own mechanism is NOT isolated to an in-phase
    # re-arm (#1204 review round 3 item 2 -- corrects an earlier, wrong
    # causal claim that used to sit here): on main, nothing in this phase
    # stops or re-arms cratedigger-unfindable.timer at all, so no
    # switch-to-configuration call there ever restarted it -- and the
    # journal places the live fire (VM-time 241.18s) inside the
    # startup-probe stretch (the pipeline_before_probe scenario below),
    # which contains no switch-to-configuration call whatsoever. The timer
    # was armed BEFORE this phase (either at ordinary VM boot or by an
    # earlier deploy-hold release restarting TIMER_UNITS elsewhere in this
    # test file) and simply elapsed, after its own RandomizedDelaySec
    # jitter, at whatever real wall-clock moment that landed -- which
    # happened to fall inside the probe scenario's own open equality
    # window. Honest framing: the incident proves the timer can elapse at
    # an arbitrary, unattributable moment during this phase; it does NOT
    # prove a specific re-arm caused it. Whenever it fires, even against a
    # DB with zero unfindable candidates, migration 077 (#1112)'s
    # unfindable_run_metrics write is attempted once per batch pass that
    # actually starts, and emptiness alone does not suppress that write
    # (scripts/run_unfindable_detection.py ~29-31, 204-215 -- only an abort
    # before any probe, or a swallowed DB error on the insert itself, skips
    # the row; neither applies to a clean empty run) -- so that one empty
    # run broke the _pipeline_data_snapshot() equality assert below it. The
    # main cratedigger.timer has no equivalent exposure because this test's
    # own VM config deliberately keeps it far from firing via BOTH
    # `onBootSec = "1d"` AND `onUnitInactiveSec = "1d"`; cratedigger-unfindable
    # has no such isolation.
    #
    # Backdate the persistent stamp (#1204 review F2) so this phase's first
    # re-arm opportunity is guaranteed overdue rather than dependent on
    # wall-clock luck -- this deliberately MANUFACTURES the overdue
    # condition for THIS test's in-phase re-arms, a different and stronger
    # exposure than the historical incident's own unattributed elapse
    # above; it is what makes this fix's correctness provable rather than
    # merely plausible. Confirmed empirically: with this backdate in place,
    # the very next re-arm -- switch1 (hard)'s own switch-to-configuration
    # reconciliation, below -- triggered a full natural empty run, including
    # its unfindable_run_metrics write, entirely INSIDE that
    # switch-to-configuration subprocess call, before this phase's own
    # Python-level _quiesce_unfindable() call ever got control back. #1204
    # review F1's move of pipeline_before_hard's capture to AFTER that
    # switch's own quiesce (not before the switch) is exactly why this real
    # race did not break the equality assert below it; the pre-F1 ordering
    # would have missed that row in the baseline and failed here. The
    # persisted stamp is written at TRIGGER time -- the instant the timer
    # fires and the service transitions to running -- not on service
    # completion (#1204 review round 3 item 4 -- corrects an earlier,
    # imprecise "a completed run refreshes" claim here): a SIGTERM'd or
    # condition-skipped run clears the overdue condition exactly as a fully
    # completed one does, since the write happens before the service does
    # any actual work. So the timer does NOT stay overdue for the rest of
    # the phase once ANY trigger happens, completed or not -- confirmed
    # empirically that switch2 (warning), switch3 (safe), and the original
    # phase-end restart (before #1204 review round 3 item 1's
    # forward-touch, below) all stayed quiet -- no further natural trigger
    # -- for the remainder of that test run. _quiesce_unfindable() after
    # every switch-to-configuration call is required regardless of whether
    # any given re-arm happens to race a trigger -- F6's guard above, not
    # this backdate on its own, is the actual deterministic backstop.
    # Stopping the timer alone is not enough -- an already-queued/running
    # service start is not cancelled by stopping its timer -- so both are
    # stopped here via _quiesce_unfindable(), and again immediately after
    # each of the three switch-to-configuration calls below, so no whole-DB
    # equality window in this phase ever runs with the timer live. Only the
    # timer is restarted, once, at the very end of the phase (forward-
    # touching the stamp first -- see that comment for why). U13 above only
    # asserts the timer is *enabled* (wantedBy timers.target); `systemctl
    # stop` does not change is-enabled, so that assertion is unaffected by
    # any of this.
    machine.succeed(
        "install -d -m 0755 /var/lib/systemd/timers; "
        "touch -d '8 days ago' "
        "/var/lib/systemd/timers/stamp-cratedigger-unfindable.timer"
    )
    machine.succeed(
        "systemctl stop cratedigger-web.service cratedigger-importer.service "
        "cratedigger-import-preview-worker.service "
        "cratedigger-youtube-ingest.service"
    )
    _quiesce_unfindable()
    safe_system = machine.succeed("readlink -f /run/current-system").strip()
    hard_system = machine.succeed(
        "readlink -f /run/current-system/specialisation/cratedigger-beets-hard"
    ).strip()
    warning_system = machine.succeed(
        "readlink -f /run/current-system/specialisation/"
        "cratedigger-beets-warning"
    ).strip()
    beets_before_hard = _beets_world_digest()

    # Keep every unrelated application stopped while installing the invalid
    # authority, then exercise one exact startup attempt with retries disabled.
    machine.succeed(f"{hard_system}/bin/switch-to-configuration test")
    _quiesce_unfindable()
    # #1204 review F1: captured here, immediately after the switch and its
    # re-quiesce, not before the switch -- capturing beforehand would open
    # this equality window while the switch's own reconciliation still had
    # the timer live (see _quiesce_unfindable()'s docstring for the general
    # ordering contract this follows). This narrows what the equality assert
    # below covers, and that narrowing is deliberate (#1204 review round 3
    # item 7 -- states the trade explicitly rather than "still supports it"
    # as an earlier version of this comment claimed): the old, pre-#1204
    # window captured this baseline BEFORE the switch, so its assert
    # incidentally also covered whatever DB effect the switch's OWN
    # reconciliation restarts of the four workers might have had while
    # running under the invalid hard authority. That incidental coverage is
    # gone now -- the assert below covers only the explicitly started
    # preview-worker's own attempt. This is the correct trade: the old
    # window's broader incidental coverage came at the cost of a live race
    # against the switch's own timer reactivation (#1204 review F1), a real
    # defect this fix reproduced live during development, not a
    # hypothetical one.
    pipeline_before_hard = _pipeline_data_snapshot()
    machine.succeed(
        "systemctl start --no-block cratedigger-import-preview-worker.service"
    )
    machine.wait_until_succeeds(
        "systemctl is-failed cratedigger-import-preview-worker.service"
    )
    hard_invocation = machine.succeed(
        "systemctl show cratedigger-import-preview-worker.service "
        "-p InvocationID --value"
    ).strip()
    hard_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={hard_invocation} -o cat"
    )
    assert hard_log.count(
        "Beets configuration rejected [import_write_disabled]"
    ) == 1, hard_log
    assert "Beets configuration admitted for preview" not in hard_log, hard_log
    assert _beets_world_digest() == beets_before_hard
    assert _pipeline_data_snapshot() == pipeline_before_hard

    machine.succeed(f"{warning_system}/bin/switch-to-configuration test")
    _quiesce_unfindable()
    machine.succeed(
        "systemctl reset-failed cratedigger-import-preview-worker.service; "
        "systemctl start cratedigger-import-preview-worker.service"
    )
    machine.wait_for_unit("cratedigger-import-preview-worker.service")
    warning_invocation = machine.succeed(
        "systemctl show cratedigger-import-preview-worker.service "
        "-p InvocationID --value"
    ).strip()
    # This unit is Type=simple, so systemd reports "active" the moment it
    # execs -- before the Python interpreter has run the admission check at
    # all. Reading the journal here samples a window the worker has not yet
    # written to, which is why the exact-count assertions below flaked to 0
    # on a slow (TCG-emulated) host while passing under KVM (#1130). The
    # admission line is emitted AFTER every warning line
    # (lib/beets_startup.py), so waiting for it proves both are present and
    # keeps the counts exact rather than weakening them to ">= 1". Same
    # barrier as the safe/recovered admission sites below.
    machine.wait_until_succeeds(
        f"journalctl _SYSTEMD_INVOCATION_ID={warning_invocation} -o cat "
        "| grep -q 'Beets configuration admitted for preview'"
    )
    warning_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={warning_invocation} -o cat"
    )
    assert warning_log.count(
        "Beets configuration warning [musicbrainz_endpoint_drift]"
    ) == 1, warning_log
    assert warning_log.count(
        "Beets configuration admitted for preview"
    ) == 1, warning_log
    machine.succeed("systemctl stop cratedigger-import-preview-worker.service")

    machine.succeed(f"{safe_system}/bin/switch-to-configuration test")
    _quiesce_unfindable()
    # A missing external state authority must reach Cratedigger's intrinsic
    # admission check. The systemd missing-path modifier prevents namespace
    # setup from failing first; the checker rejects the exact absent authority
    # without creating it or mutating the catalog/library/pipeline world.
    beets_before_missing_state = _beets_world_digest()
    pipeline_before_missing_state = _pipeline_data_snapshot()
    machine.succeed(
        "systemctl stop cratedigger-importer.service "
        "cratedigger-import-preview-worker.service cratedigger-web.service; "
        "install -d /run/systemd/system/"
        "cratedigger-import-preview-worker.service.d; "
        "printf '[Service]\\nRestart=no\\n' > /run/systemd/system/"
        "cratedigger-import-preview-worker.service.d/missing-state.conf; "
        "systemctl daemon-reload; "
        "mv ${externalStateFile} ${externalStateFile}.missing; "
        "systemctl reset-failed cratedigger-import-preview-worker.service; "
        "systemctl start --no-block cratedigger-import-preview-worker.service"
    )
    machine.wait_until_succeeds(
        "systemctl is-failed cratedigger-import-preview-worker.service"
    )
    missing_state_invocation = machine.succeed(
        "systemctl show cratedigger-import-preview-worker.service "
        "-p InvocationID --value"
    ).strip()
    missing_state_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={missing_state_invocation} -o cat"
    )
    assert missing_state_log.count(
        "Beets configuration rejected [state_not_regular]"
    ) == 1, missing_state_log
    assert "Beets configuration admitted for preview" not in missing_state_log, missing_state_log
    machine.fail("test -e ${externalStateFile}")
    assert _pipeline_data_snapshot() == pipeline_before_missing_state
    machine.succeed(
        "mv ${externalStateFile}.missing ${externalStateFile}; "
        "rm -r /run/systemd/system/"
        "cratedigger-import-preview-worker.service.d; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger-import-preview-worker.service"
    )
    assert _beets_world_digest() == beets_before_missing_state
    machine.succeed(
        "systemctl reset-failed cratedigger-import-preview-worker.service; "
        "systemctl start cratedigger-import-preview-worker.service"
    )
    machine.wait_for_unit("cratedigger-import-preview-worker.service")
    safe_invocation = machine.succeed(
        "systemctl show cratedigger-import-preview-worker.service "
        "-p InvocationID --value"
    ).strip()
    machine.wait_until_succeeds(
        f"journalctl _SYSTEMD_INVOCATION_ID={safe_invocation} -o cat "
        "| grep -q 'Beets configuration admitted for preview'"
    )
    safe_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={safe_invocation} -o cat"
    )
    assert safe_log.count(
        "Beets configuration admitted for preview"
    ) == 1, safe_log
    assert "Beets configuration rejected" not in safe_log, safe_log
    assert _beets_world_digest() == beets_before_hard

    # A lexical library path that resolves through a symlink to / must not
    # turn the importer's narrow ReadWritePaths capability into a root-wide
    # mutation namespace. Intrinsic admission rejects the resolved authority
    # before the worker reaches any PipelineDB or Beets operation.
    machine.succeed("systemctl stop cratedigger-import-preview-worker.service")
    beets_before_root_alias = _beets_world_digest()
    pipeline_before_root_alias = _pipeline_data_snapshot()
    machine.succeed(
        "install -d /run/systemd/system/cratedigger-importer.service.d; "
        "printf '[Service]\\nRestart=no\\nExecStartPre=\\n' > /run/systemd/system/"
        "cratedigger-importer.service.d/root-alias.conf; "
        "mv ${externalLibraryRoot} ${externalLibraryRoot}.safe; "
        "ln -s / ${externalLibraryRoot}; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger-importer.service; "
        "systemctl start --no-block cratedigger-importer.service"
    )
    machine.wait_until_succeeds("systemctl is-failed cratedigger-importer.service")
    root_alias_invocation = machine.succeed(
        "systemctl show cratedigger-importer.service -p InvocationID --value"
    ).strip()
    root_alias_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={root_alias_invocation} -o cat"
    )
    assert root_alias_log.count(
        "Beets configuration rejected [directory_root]"
    ) == 1, root_alias_log
    assert "Beets configuration admitted for importer" not in root_alias_log, root_alias_log
    assert _pipeline_data_snapshot() == pipeline_before_root_alias
    machine.succeed(
        "rm ${externalLibraryRoot}; "
        "mv ${externalLibraryRoot}.safe ${externalLibraryRoot}; "
        "rm -r /run/systemd/system/cratedigger-importer.service.d; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger-importer.service"
    )
    assert _beets_world_digest() == beets_before_root_alias

    # Issue #1085: a unit that cannot use a required path fails at switch
    # time, loudly -- the container-entrypoint pattern -- instead of
    # discovering the problem later one operation at a time. Representative
    # case: the importer's canonical processing write/read target
    # (/var/lib/cratedigger/processing/albums, already proven 0700
    # cratedigger:cratedigger above) becomes unreachable.
    beets_before_probe = _beets_world_digest()
    pipeline_before_probe = _pipeline_data_snapshot()
    machine.succeed(
        "systemctl stop cratedigger-importer.service; "
        "chmod 000 /var/lib/cratedigger/processing/albums; "
        "install -d /run/systemd/system/"
        "cratedigger-importer.service.d; "
        "printf '[Service]\\nRestart=no\\n' > /run/systemd/system/"
        "cratedigger-importer.service.d/startup-probe.conf; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger-importer.service; "
        "systemctl start --no-block cratedigger-importer.service"
    )
    machine.wait_until_succeeds(
        "systemctl is-failed cratedigger-importer.service"
    )
    probe_invocation = machine.succeed(
        "systemctl show cratedigger-importer.service "
        "-p InvocationID --value"
    ).strip()
    probe_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={probe_invocation} -o cat"
    )
    # Beets admission succeeded (this is not a Beets-authority rejection);
    # the startup write-probe is what stops the unit, before any queue
    # recovery/claim/DB mutation.
    assert probe_log.count(
        "Beets configuration admitted for importer"
    ) == 1, probe_log
    assert (
        "startup open probe failed at "
        "/var/lib/cratedigger/processing/albums [EACCES]"
    ) in probe_log, probe_log
    assert _pipeline_data_snapshot() == pipeline_before_probe
    machine.succeed(
        "chmod 700 /var/lib/cratedigger/processing/albums; "
        "rm -r /run/systemd/system/cratedigger-importer.service.d; "
        "systemctl daemon-reload; "
        "systemctl reset-failed cratedigger-importer.service; "
        "systemctl start cratedigger-importer.service"
    )
    machine.wait_for_unit("cratedigger-importer.service")
    recovered_invocation = machine.succeed(
        "systemctl show cratedigger-importer.service "
        "-p InvocationID --value"
    ).strip()
    # wait_for_unit only proves systemd's own "active" state; the
    # admission log line can land a moment after that.
    machine.wait_until_succeeds(
        f"journalctl _SYSTEMD_INVOCATION_ID={recovered_invocation} -o cat "
        "| grep -q 'Beets configuration admitted for importer'"
    )
    recovered_log = machine.succeed(
        f"journalctl _SYSTEMD_INVOCATION_ID={recovered_invocation} -o cat"
    )
    # The natural retry starts cleanly: admitted once, no probe failure.
    assert recovered_log.count(
        "Beets configuration admitted for importer"
    ) == 1, recovered_log
    assert "probe failed" not in recovered_log, recovered_log
    assert _beets_world_digest() == beets_before_probe

    # Re-arm the timer this phase's whole-DB equality window quiesced above.
    # Every remaining pg_dump-equality assert has now run; nothing between
    # here and the real reboot further down needs the timer stopped, and
    # that reboot scenario proves it back to ActiveState=active regardless
    # (systemd starts every wantedBy=timers.target unit on boot). Only the
    # timer needs restarting -- the oneshot service itself has no
    # persistent "stopped" state to restore, and no later assertion checks
    # cratedigger-unfindable.service's ActiveState.
    #
    # Forward-touch the persistent stamp to "now" immediately before this
    # restart (#1204 review round 3 item 1 -- replaces an earlier,
    # luck-based claim that switch1's natural trigger above already
    # consumed the overdue condition for the rest of this boot). That
    # consumption is a per-run race, not a guarantee: RandomizedDelaySec
    # draws up to 30 minutes against this whole VM test's few-minute
    # budget, so on a run where switch1's own race does NOT trigger, the
    # stamp is still 8 days old here and this restart would itself be
    # eligible for its own catch-up elapse. Forward-touching makes the
    # timer's post-restart state a mechanism, not a coin flip: a current
    # stamp means the next OnCalendar=daily boundary is a genuine ~24h
    # away (plus jitter), so this restart cannot produce a stray run later
    # in this file -- including squeezing the two `timeout 120 ...
    # cratedigger-deploy-hold acquire` budgets and the untimed
    # prepare-controlled call further down, both of which drain this exact
    # timer's own producer queue as part of their real production
    # behaviour and have no reason to expect it eager -- or surviving the
    # real reboot near the end of this test to fire unexpectedly on the
    # fresh boot.
    machine.succeed(
        "touch /var/lib/systemd/timers/stamp-cratedigger-unfindable.timer"
    )
    machine.succeed("systemctl start cratedigger-unfindable.timer")

    readiness_log = machine.succeed(
        "journalctl -b -u cratedigger-test-beets-readiness.service -o cat"
    )
    # Initial activation, healthy lifecycle restart, then recovery from the
    # deliberately failed producer world.
    assert readiness_log.count("BEETS_EXTERNAL_READINESS_OK") == 3, readiness_log

    # #1096: acquire's producer-drain-before-hold window owns no persistent
    # object at all (#1078), so the only reboot exposure left is the
    # persistent manual gate hold and producer start inhibitors -- both
    # live under /var/lib/cratedigger-metadata-gate, which (unlike /run)
    # survives a real QEMU crash+restart, because this VM's root filesystem
    # is the default persistent qcow2 disk (virtualisation.diskImage), not
    # tmpfs. Deliberately the LAST group of scenarios this whole test does
    # (this one and its #1096 correction-round M1+M2 sibling immediately
    # below): earlier scenarios above this point count service restarts and
    # journal messages "since boot" (BEETS_EXTERNAL_READINESS_OK above,
    # "Beets configuration admitted" counts elsewhere) -- a real reboot
    # resets that boot marker and a reboot scenario's own test-fixture
    # worker restarts would otherwise inflate those counts out from under
    # assertions such a scenario has no other reason to know about. Placed
    # last, neither this scenario's nor its sibling's reboots and restarts
    # can ever be observed by anything; the sibling scenario's own second
    # reboot is symmetrically safe for the identical reason, since nothing
    # follows it either.
    #
    # Drive a real acquire through prepare-controlled -- reusing the phase
    # the first deploy-hold scenario reaches at prepare-controlled/
    # open-main-timer -- then reboot for real, and prove a receiptless
    # abort adopts the surviving YouTube inhibitor and its new persistent
    # ownership marker, restoring ordinary operation with no receipt at
    # all. Nothing needs to continue after this, so there is no matching
    # re-acquire/re-release: ordinary operation is this test's own final
    # state.
    #
    # prepared-controlled is deliberately the phase under test, not held:
    # prepare_controlled releases the manual hold (and its persistent
    # marker) before this phase is ever written, so only the YouTube start
    # inhibitor -- and its sibling persistent marker -- remain owned across
    # it. That is the genuinely novel, multi-phase-surviving case #1096
    # exists for; the manual-hold adoption branch reuses the exact
    # restart-and-prove shape abort_hold's existing manual-hold branch
    # already exercises at the unit level
    # (tests/test_deploy_hold.py::TestReceiptlessAbortAdoptsPersistentMarkers).
    machine.fail("test -e /run/cratedigger-deploy-hold")

    # The synthetic metadata-gate fixture (metadataGateTool, above) only
    # records the hold marker file; unlike the real deployment-owned gate
    # tool it never stops already-running units (documented at length where
    # the #1098 abort scenario above hits the identical divergence). This
    # test does not know whether the four gate-guarded workers are
    # currently active this late in the file, so stop them unconditionally
    # first -- systemctl stop on an already-inactive unit is a harmless
    # no-op -- exactly as the #1098 scenario does before its own acquire
    # call, so a fresh acquire's own SERVICE_UNITS drain observes them
    # already inactive instead of waiting the full 7200s timeout for a stop
    # nothing in this fixture ever performs.
    machine.succeed(
        "systemctl stop cratedigger-web.service cratedigger-importer.service "
        "cratedigger-import-preview-worker.service "
        "cratedigger-youtube-ingest.service"
    )
    # (Re-)provision the pgpass secret the lifecycle-preflight query reads
    # (the VM uses peer auth, so the value is intentionally synthetic while
    # the boundary is real); harmless if a still-valid copy already exists
    # from earlier in this test.
    machine.succeed("install -d -o root -g root -m 0700 /run/secrets")
    machine.succeed(
        "printf 'PGPASSWORD=module-vm-unused\\n' "
        "> /run/secrets/cratedigger-pgpass"
    )
    machine.succeed("chmod 0400 /run/secrets/cratedigger-pgpass")

    machine.succeed("timeout 120 cratedigger-deploy-hold acquire")
    machine.succeed("cratedigger-deploy-hold prepare-controlled")
    reboot_phase = machine.succeed(
        "cat /run/cratedigger-deploy-hold/phase"
    ).strip()
    assert reboot_phase == "prepared-controlled", reboot_phase
    machine.succeed(
        "test -f /run/cratedigger-deploy-hold/owned-inhibitor-"
        "cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/"
        "deploy-hold-owned-inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )
    machine.succeed("test ! -e /var/lib/cratedigger-metadata-gate/holds/manual")
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "active", (service, state)

    # The real reboot: a graceful poweroff (not machine.crash(), which
    # simulates an abrupt power failure and can therefore surface ordinary
    # ext4 write-back-caching non-durability of a very recent unlink() --
    # ext4 crash-consistency is a real but entirely separate concern from
    # #1096, which is about what /run tmpfs does and does not carry across
    # an ordinary reboot). shutdown() sends the guest a clean poweroff,
    # unmounting and syncing filesystems the way an operator-initiated
    # reboot does, before start() boots a fresh instance against the same
    # persistent disk.
    machine.shutdown()
    machine.start()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")

    # /run is tmpfs and did not survive the reboot: the receipt, every
    # tmpfs ownership marker, and both remaining timer control-link masks
    # are gone, and every timer is back to its ordinary loaded/active state
    # -- unlike a receipt-owned mask, a timer control-link needs no
    # persistent marker to recover, because tmpfs on both sides means there
    # is nothing here to adopt in the first place.
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.succeed(f"test ! -e /run/systemd/system.control/{timer}")
        timer_load_state = machine.succeed(
            f"systemctl show {timer} --property=LoadState --value"
        ).strip()
        assert timer_load_state == "loaded", (timer, timer_load_state)
        timer_active_state = machine.succeed(
            f"systemctl show {timer} --property=ActiveState --value"
        ).strip()
        assert timer_active_state == "active", (timer, timer_active_state)

    # /var/lib survived: the YouTube inhibitor and its new persistent
    # ownership marker are both exactly where prepare-controlled left them,
    # self-describing this receiptless world as ours; the main inhibitor
    # (already released before prepare-controlled wrote this phase) and the
    # manual hold (released even earlier) are correctly still absent.
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/"
        "deploy-hold-owned-inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )
    machine.succeed("test ! -e /var/lib/cratedigger-metadata-gate/holds/manual")
    youtube_state_before_adopt = machine.succeed(
        "systemctl show cratedigger-youtube-ingest.service "
        "--property=ActiveState --value"
    ).strip()
    assert youtube_state_before_adopt == "inactive", youtube_state_before_adopt

    # Before adoption: acquire must refuse this world and point the operator
    # at abort, not silently step around a hold it does not itself hold a
    # receipt for. Needs nothing from pipeline-cli -- the refusal is proven
    # before acquire ever reaches its lifecycle-preflight query.
    reboot_reacquire_status, reboot_reacquire_output = machine.execute(
        "timeout 20 cratedigger-deploy-hold acquire 2>&1"
    )
    assert reboot_reacquire_status != 0, reboot_reacquire_output
    assert "run 'abort'" in reboot_reacquire_output, reboot_reacquire_output
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")

    # This module-vm.nix fixture's synthetic first-boot config-hold gate
    # (configHoldGate, above) is wantedBy multi-user.target with no
    # first-boot guard of its own, so it recreates
    # /run/cratedigger-test-config-hold on every boot -- including this one
    # -- re-blocking every held application unit exactly as it did on the
    # VM's true first boot (a module-vm.nix-only artifact standing in for a
    # real downstream readiness gate, not anything cratedigger-deploy-hold
    # owns). Clear it again, and confirm the readiness fixture -- also
    # wantedBy multi-user.target -- completed again before trusting anything
    # downstream of it.
    machine.wait_for_unit("cratedigger-test-config-hold.service")
    machine.succeed("test -f /run/cratedigger-test-config-hold")
    machine.wait_for_unit("cratedigger-test-beets-readiness.service")
    reboot_readiness_state = machine.succeed(
        "systemctl is-active cratedigger-test-beets-readiness.service"
    ).strip()
    assert reboot_readiness_state == "active", reboot_readiness_state
    machine.succeed("rm /run/cratedigger-test-config-hold")

    # The config-hold gate condition-skipped these three controlled workers
    # at this boot's one and only start attempt, before this test ever
    # removed the marker above -- systemd does not retry a
    # condition-skipped unit on its own. Production carries no such extra
    # gate; restarting them here is this fixture's own workaround, not
    # evidence about abort's restart set (already covered at the unit level
    # by TestReceiptlessAbortAdoptsPersistentMarkers, which restarts exactly
    # GATE_STOPPED_UNITS only when the manual hold marker is present -- not
    # the case at this phase).
    machine.succeed(
        "systemctl start cratedigger-web.service "
        "cratedigger-importer.service "
        "cratedigger-import-preview-worker.service"
    )
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
    ):
        machine.wait_until_succeeds(
            f"systemctl show {service} --property=ActiveState --value "
            "| grep -qx active"
        )

    # The adoption itself: no receipt, only the persistent markers above.
    machine.succeed("timeout 60 cratedigger-deploy-hold abort")

    # Ordinary operation, genuinely restored with no receipt at all.
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/"
        "inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/"
        "deploy-hold-owned-inhibit-cratedigger-youtube-ingest.service"
    )
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )
    machine.succeed("test ! -e /var/lib/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        timer_state = machine.succeed(
            f"systemctl show {timer} --property=ActiveState --value"
        ).strip()
        assert timer_state == "active", (timer, timer_state)
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "active", (service, state)
    machine.succeed("cratedigger-metadata-gate start-check")

    # #1096 correction round (M1+M2): the scenario above only ever leaves
    # the YouTube inhibitor and its persistent marker surviving a reboot --
    # prepare-controlled had already released the manual hold and main's
    # own inhibitor by the time it wrote prepared-controlled. Independent
    # review found two real-systemd hangs in exactly the WIDER window this
    # scenario never reaches: a reboot after prepare-controlled's ``for
    # service in START_INHIBITORS: _ensure_owned_start_inhibitor(...)``
    # loop creates BOTH the main and YouTube inhibitors, but before it ever
    # calls ``metadata_gate("release manual")``, leaves the manual hold,
    # the main inhibitor, and the YouTube inhibitor ALL persistently marked
    # together. M1: naming ``cratedigger.service`` -- a Type=oneshot that
    # can never reach active/running -- among the units
    # ``_wait_controlled_workers_active`` proves hangs for the full
    # ``_DRAIN_TIMEOUT_SECONDS`` bound (2h, 7200 polls -- NOT the 6h
    # ``_PRODUCER_DRAIN_TIMEOUT_SECONDS``, which nothing in this call ever
    # uses), inhibitor files already deleted, every rerun identical. M2:
    # releasing the manual hold and restarting
    # ``GATE_STOPPED_UNITS`` (which includes YouTube ingest) BEFORE
    # removing the still-present YouTube inhibitor file lets real systemd's
    # ConditionPathExists condition-skip that very restart -- ``systemctl
    # start`` exits 0, the unit stays down -- so the wait times out
    # identically. The structural fix removes every marked inhibitor file
    # and releases the marked manual hold before a single restart-and-prove
    # pass over ``GATE_STOPPED_UNITS | (inhibited_marked - {MAIN_SERVICE})``,
    # starting ``cratedigger.service`` separately with no wait; this
    # scenario proves that fix against real systemd rather than the Python
    # fake the unit tests above (TestReceiptlessAbortAdoptsPersistentMarkers)
    # already qualify with planted mutants.
    #
    # Reaching this exact crash window through prepare-controlled itself
    # would mean interrupting a live subprocess at one specific Python
    # statement -- fragile and racy in a VM. Constructing the filesystem
    # delta directly is the faithful, deterministic alternative: the
    # persistent-marker mechanism is deliberately designed to trust file
    # presence regardless of how it got there (#1096), so acquiring HELD
    # for real (which genuinely marks and activates the manual hold) and
    # then hand-writing exactly the two inhibitor files and their two
    # persistent markers prepare-controlled's own inhibitor loop would have
    # written is the identical world a genuine crash there would leave --
    # exercised against real systemd rather than re-driving the fake.
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "active", (service, state)

    machine.succeed(
        "systemctl stop cratedigger-web.service cratedigger-importer.service "
        "cratedigger-import-preview-worker.service "
        "cratedigger-youtube-ingest.service"
    )
    # /run was wiped by the first reboot above and never re-provisioned;
    # acquire's lifecycle-preflight query needs it again.
    machine.succeed("install -d -o root -g root -m 0700 /run/secrets")
    machine.succeed(
        "printf 'PGPASSWORD=module-vm-unused\\n' "
        "> /run/secrets/cratedigger-pgpass"
    )
    machine.succeed("chmod 0400 /run/secrets/cratedigger-pgpass")

    machine.succeed("timeout 120 cratedigger-deploy-hold acquire")
    m1m2_held_phase = machine.succeed(
        "cat /run/cratedigger-deploy-hold/phase"
    ).strip()
    assert m1m2_held_phase == "held", m1m2_held_phase
    machine.succeed("test -f /var/lib/cratedigger-metadata-gate/holds/manual")
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )

    # By hand, create exactly the filesystem delta prepare-controlled's own
    # inhibitor loop makes -- persistent marker before the inhibitor file
    # itself, mirroring mark_inhibitor_owned()/create_start_inhibitor()
    # (#1096) -- for BOTH main and YouTube, without ever invoking
    # prepare-controlled itself.
    for service in ("cratedigger.service", "cratedigger-youtube-ingest.service"):
        machine.succeed(
            f"printf '{service}\\n' > /var/lib/cratedigger-metadata-gate/"
            f"deploy-hold-owned-inhibit-{service}"
        )
        machine.succeed(
            "chmod 0600 /var/lib/cratedigger-metadata-gate/"
            f"deploy-hold-owned-inhibit-{service}"
        )
        machine.succeed(
            "printf 'cratedigger-deploy-hold-v1\\n' "
            f"> /var/lib/cratedigger-metadata-gate/inhibit-{service}"
        )
        machine.succeed(
            f"chmod 0600 /var/lib/cratedigger-metadata-gate/inhibit-{service}"
        )

    # The second real reboot: identical justification to the first
    # (machine.shutdown(), never machine.crash() -- see that scenario's own
    # comment on ext4 write-back-caching non-durability being an unrelated
    # concern from #1096).
    machine.shutdown()
    machine.start()
    machine.wait_for_unit("multi-user.target")
    machine.wait_for_unit("postgresql.service")

    # /run is gone again; every hand-written /var/lib artifact above
    # survived, exactly like the simpler single-inhibitor case.
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    for service in ("cratedigger.service", "cratedigger-youtube-ingest.service"):
        machine.succeed(
            f"test -f /var/lib/cratedigger-metadata-gate/inhibit-{service}"
        )
        machine.succeed(
            "test -f /var/lib/cratedigger-metadata-gate/"
            f"deploy-hold-owned-inhibit-{service}"
        )
    machine.succeed("test -f /var/lib/cratedigger-metadata-gate/holds/manual")
    machine.succeed(
        "test -f /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )

    # acquire must refuse and point at abort, exactly as the simpler case.
    m1m2_reacquire_status, m1m2_reacquire_output = machine.execute(
        "timeout 20 cratedigger-deploy-hold acquire 2>&1"
    )
    assert m1m2_reacquire_status != 0, m1m2_reacquire_output
    assert "run 'abort'" in m1m2_reacquire_output, m1m2_reacquire_output
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")

    # Clear this module-vm.nix-only fixture's one-shot first-boot marker so
    # it is not an extra confounding condition-skip source once abort tries
    # to start these units below -- unlike the simpler single-inhibitor
    # scenario above, this scenario does NOT need to restart the three
    # workers by hand here: the manual hold this scenario deliberately kept
    # active is what actually condition-skips them at this boot's one
    # start attempt, not merely this fixture's marker, and the hold stays
    # active until abort itself releases it below. Proving that abort's own
    # restart -- not a hand workaround -- is what brings them up is exactly
    # what this scenario exists to test.
    machine.wait_for_unit("cratedigger-test-config-hold.service")
    machine.succeed("test -f /run/cratedigger-test-config-hold")
    machine.wait_for_unit("cratedigger-test-beets-readiness.service")
    machine.succeed("rm /run/cratedigger-test-config-hold")

    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "inactive", (service, state)
    main_state_before_m1m2_adopt = machine.succeed(
        "systemctl show cratedigger.service --property=ActiveState --value"
    ).strip()
    assert main_state_before_m1m2_adopt == "inactive", main_state_before_m1m2_adopt

    # The M1+M2 adoption itself: manual hold, main inhibitor, and YouTube
    # inhibitor ALL persistently marked together, restarting all four
    # gate-guarded units from cold (unlike the simpler scenario above,
    # which only asks abort to restart YouTube). Bounded well under the
    # pre-fix failure mode (a full 7200s _DRAIN_TIMEOUT_SECONDS wait) so an
    # unfixed ordering fails this assertion via timeout, not a hang of the
    # test suite itself.
    machine.succeed("timeout 120 cratedigger-deploy-hold abort")

    # Every marker, inhibitor file, and the hold itself are gone; the four
    # gate-guarded units are active -- proving M2's fix (the YouTube
    # restart was not condition-skipped by its own now-removed inhibitor).
    machine.succeed("test ! -e /run/cratedigger-deploy-hold")
    for service in ("cratedigger.service", "cratedigger-youtube-ingest.service"):
        machine.succeed(
            f"test ! -e /var/lib/cratedigger-metadata-gate/inhibit-{service}"
        )
        machine.succeed(
            "test ! -e /var/lib/cratedigger-metadata-gate/"
            f"deploy-hold-owned-inhibit-{service}"
        )
    machine.succeed("test ! -e /var/lib/cratedigger-metadata-gate/holds/manual")
    machine.succeed(
        "test ! -e /var/lib/cratedigger-metadata-gate/deploy-hold-owned-manual"
    )
    for service in (
        "cratedigger-web.service",
        "cratedigger-importer.service",
        "cratedigger-import-preview-worker.service",
        "cratedigger-youtube-ingest.service",
    ):
        state = machine.succeed(
            f"systemctl show {service} --property=ActiveState --value"
        ).strip()
        assert state == "active", (service, state)

    # cratedigger.service proving M1's fix: abort started it unproven and
    # returned without waiting on it. A real cycle here has no slskd to
    # talk to and will eventually fail on that unrelated ground (out of
    # this scenario's contract, same as the module-vm main-service scenario
    # far above) -- what this scenario needs is only that it was actually
    # STARTED for real rather than condition-skipped by its own
    # already-removed inhibitor. This boot never started main before now
    # (proven above: main_state_before_m1m2_adopt == "inactive", and this
    # is a fresh boot with nothing else in this scenario starting it), so
    # "-b" alone -- the identical pattern the module-vm main-service
    # scenario far above already uses -- unambiguously scopes the search to
    # abort's own start.
    machine.wait_until_succeeds(
        "journalctl -b -u cratedigger.service -o cat "
        "| grep -q 'Beets configuration admitted for main'"
    )
    machine.succeed(
        "systemctl kill --kill-whom=all --signal=SIGKILL cratedigger.service || true"
    )
    machine.succeed("systemctl reset-failed cratedigger.service || true")

    machine.succeed("cratedigger-metadata-gate start-check")

    # --- issue #1161: a switch must re-run the migrator even when an
    # unrelated `systemctl start` lands mid-switch ---
    #
    # Kept last in this script: the restart below deliberately bounces the
    # four Requires= dependents, and nothing after it asserts on them.
    #
    # The real adapter is the unit FILE switch-to-configuration parses --
    # X- keys are not systemd properties, so `systemctl show` cannot see
    # this and only `systemctl cat` can. NixOS renders stopIfChanged = false
    # as X-StopIfChanged=false, and switch-to-configuration-ng reads that key
    # from the [Service] section to route a changed unit to its restart list
    # instead of its stop+start lists.
    migrate_unit_file = machine.succeed("systemctl cat cratedigger-db-migrate.service")
    assert "[Service]" in migrate_unit_file, migrate_unit_file
    # Bounded at the next section header -- an unbounded split would run to EOF
    # and also span [Install], which would not prove "in [Service]" at all.
    migrate_service_section = (
        migrate_unit_file.split("[Service]", 1)[1].split("\n[", 1)[0]
    )
    assert "X-StopIfChanged=false" in migrate_service_section, migrate_unit_file

    # The outermost real adapter: switch-to-configuration itself. The unit-file
    # pin above proves we EMIT the key; this proves the switch HONOURS it, and
    # is what fails if a future nixpkgs bump changes that handling (this repo
    # updates nixpkgs daily, so silently losing the protection is a real risk).
    # dry-activate only prints its plan, so it cannot disturb the VM.
    migrate_bump_system = machine.succeed(
        "readlink -f /run/current-system/specialisation/cratedigger-migrate-bump"
    ).strip()
    routing_plan = machine.succeed(
        f"{migrate_bump_system}/bin/switch-to-configuration dry-activate 2>&1"
    )

    def _planned(verb):
        prefix = f"would {verb} the following units: "
        for line in routing_plan.splitlines():
            if line.startswith(prefix):
                return [u.strip() for u in line[len(prefix):].split(",")]
        return []

    assert "cratedigger-db-migrate.service" in _planned("restart"), routing_plan
    # And specifically NOT in the stop+start pair, which is the routing that
    # let a concurrent start replace the queued stop in #1161.
    assert "cratedigger-db-migrate.service" not in _planned("stop"), routing_plan
    assert "cratedigger-db-migrate.service" not in _planned("start"), routing_plan

    # Behaviour pair proving why that routing is load-bearing. Both commands
    # are synchronous for a Type=oneshot, so no settling wait is needed.
    machine.succeed("systemctl start cratedigger-db-migrate.service")
    migrate_substate = machine.succeed(
        "systemctl show cratedigger-db-migrate.service --property=SubState --value"
    ).strip()
    assert migrate_substate == "exited", migrate_substate
    migrate_baseline = machine.succeed(
        "systemctl show cratedigger-db-migrate.service --property=InvocationID --value"
    ).strip()
    assert migrate_baseline, "migrate unit should carry an InvocationID"

    # The defect mechanism: a plain start on an already-active RemainAfterExit
    # oneshot returns -EALREADY, so ExecStart never forks and systemd logs
    # nothing at all. In #1161 such a start REPLACED the switch's still-queued
    # stop job, leaving migration 078 unapplied while the unit went on
    # reporting active/exited/success from a switch 12.5 h earlier.
    machine.succeed("systemctl start cratedigger-db-migrate.service")
    migrate_after_start = machine.succeed(
        "systemctl show cratedigger-db-migrate.service --property=InvocationID --value"
    ).strip()
    assert migrate_after_start == migrate_baseline, (
        "a plain start must be a silent no-op on the active oneshot "
        f"({migrate_baseline} -> {migrate_after_start})"
    )

    # The other half of that premise: a restart -- which the dry-activate probe
    # above proves the switch now issues for this unit -- does re-run the
    # migrator. NOTE this pair tests generic Type=oneshot + RemainAfterExit
    # systemd semantics, NOT our fix: `systemctl start`/`restart` never consult
    # X-StopIfChanged, so reverting stopIfChanged = false leaves both
    # assertions green. The routing probe above is what fails on that revert.
    # This pair earns its place as evidence that the -EALREADY world in the
    # #1161 RCA is real rather than asserted, and it is the only assertion here
    # that dies if RemainAfterExit is removed.
    machine.succeed("systemctl restart cratedigger-db-migrate.service")
    migrate_after_restart = machine.succeed(
        "systemctl show cratedigger-db-migrate.service --property=InvocationID --value"
    ).strip()
    assert migrate_after_restart and migrate_after_restart != migrate_baseline, (
        "restart must re-run the migrator "
        f"({migrate_baseline} -> {migrate_after_restart})"
    )
    '';
  in ''
    exec(compile(open("${script}", encoding="utf-8").read(), "${script}", "exec"))
  '';
}
