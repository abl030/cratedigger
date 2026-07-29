# NixOS VM test for the upstream cratedigger module — the STRANGER-BOOT
# gate (tier-2 plan U10, R12): a competent NixOS stranger's first boot,
# every `nix flake check`.
#
# Posture: pipelineDb.createLocally = true (module-provisioned postgres,
# peer auth, no hand-rolled DB block), beets.validation ON, VM-local beets
# paths, NO mirror knobs (public-MB defaults), and explicit operator-group
# access to the rendered Discogs include.
#
# Verifies: migrate green behind module-owned postgres ordering; rendered
# config.ini (api keys as *File paths, [Beets] runtime keys, api_base
# defaults, socket DSN with no credentials) AND rendered beets config.yaml
# (duplicate_keys nesting, fixed plugin list, public-MB, included token);
# service and operator load the same full plugin set; the web UI boots behind
# the module-owned Basic-auth gateway and Unix socket, then transitions through
# explicit insecure mode and back without weakening the remaining perimeter;
# youtube-ingest + unfindable units structurally sound.
#
# Does NOT exercise: slskd interaction, real downloads, real imports —
# those need heavyweight fixtures that belong in the python suite.
{ pkgs, system, cratediggerModule, cratediggerSrc }:

let
  # Parses the module-rendered beets config and asserts the invariants that
  # have bitten in production: duplicate_keys nesting (Palo Santo guard),
  # the fixed plugin list with musicbrainz present (zero-candidates guard),
  # public-MB defaults and the explicit included-token shape.
  pyWithYaml = pkgs.python3.withPackages (ps: [ ps.pyyaml ]);
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
  checkRenderedBeetsConfig = pkgs.writeText "check-rendered-beets-config.py" ''
    import yaml

    with open("/var/lib/cratedigger/beets/config.yaml") as f:
        cfg = yaml.safe_load(f)

    dk = cfg["import"]["duplicate_keys"]
    assert dk["album"] == ["mb_albumid", "discogs_albumid"], dk
    assert dk["item"] == ["artist", "title"], dk

    plugins = cfg["plugins"].split()
    expected = (
        "musicbrainz discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    ).split()
    assert plugins == expected, plugins

    mb = cfg["musicbrainz"]
    assert mb["host"] == "musicbrainz.org", mb
    assert mb["https"] is True, mb
    assert mb["ratelimit"] == 1, mb

    assert cfg["include"] == ["secrets.yaml"], cfg.get("include")
    assert cfg["library"] == "/var/lib/cratedigger-beets-db/beets-library.db", cfg

    # Path-affecting keys present and production-shaped. path_disambig is
    # the never-empty aunique disambiguator (Passenger collision fix,
    # 2026-07-18) — it must appear in the template AND be defined as an
    # inline album field, or same-key sibling pressings collide into one
    # folder again.
    assert cfg["asciify_paths"] is True
    assert "path_disambig" in cfg["paths"]["default"], cfg["paths"]
    assert "path_disambig" in cfg["album_fields"], cfg.get("album_fields")

    print("BEETS_CONFIG_OK")
  '';
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
        root.chmod(0o2775)
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
    heldApplicationUnits = map (name: "${name}.service")
      heldApplicationServiceNames;
    importerSandboxProbe = pkgs.writeShellScript "cratedigger-importer-sandbox-probe" ''
      set -euo pipefail
      probe_dir=/var/lib/cratedigger/processing/sandbox-probe
      ${pkgs.coreutils}/bin/install -d -m 0700 "$probe_dir"
      test -f /var/lib/cratedigger/config.ini

      # Run representative shipped media/Beets tools inside the importer's
      # actual service sandbox and @system-service syscall filter.
      ${pkgs.sox}/bin/sox \
        -n -r 44100 -c 1 "$probe_dir/tone.wav" synth 0.05 sine 440
      ${pkgs.ffmpeg}/bin/ffmpeg \
        -nostdin -loglevel error -y -i "$probe_dir/tone.wav" \
        -codec:a libmp3lame "$probe_dir/tone.mp3"
      ${pkgs.mp3val}/bin/mp3val "$probe_dir/tone.mp3" >/dev/null
      /run/current-system/sw/bin/cratedigger-beet version >/dev/null

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
    deployHoldTool = pkgs.writeShellScriptBin "cratedigger-deploy-hold" ''
      exec ${pkgs.python3}/bin/python3 \
        ${cratediggerSrc}/scripts/cratedigger_deploy_hold.py "$@"
    '';
    metadataGateTool = pkgs.writeShellScriptBin "cratedigger-metadata-gate" ''
      set -euo pipefail
      hold_dir=/run/cratedigger-metadata-gate/holds
      case "''${1:-}" in
        hold)
          test "''${2:-}" = manual
          install -d -m 0755 "$hold_dir"
          printf 'manual\n' > "$hold_dir/manual"
          ;;
        release)
          test "''${2:-}" = manual
          rm -f "$hold_dir/manual"
          ;;
        resume-if-clear)
          test ! -e "$hold_dir/manual"
          ;;
        *) exit 64 ;;
      esac
    '';
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
    environment.etc."cratedigger/discogs-token" = {
      text = "test-discogs-token-do-not-use\n";
      mode = "0400";
      user = "cratedigger";
      group = "beets-library";
    };
    users.users.beets-operator = {
      isNormalUser = true;
      extraGroups = ["cratedigger-ops" "beets-library" "cratedigger-web"];
    };
    users.users.unrelated-user.isNormalUser = true;
    users.groups.slskd-writer = {};
    users.users.slskd-writer = {
      isSystemUser = true;
      group = "slskd-writer";
    };
    # The source-owner group is separate from the private processor group.
    # The service can consume event-stamped source bytes but never grants
    # the writer any authority over its processing root.
    users.users.cratedigger.extraGroups = [ "slskd-writer" ];
    networking.hosts."127.0.0.1" = [
      "music.vm.test"
      "unrelated.vm.test"
    ];
    security.pki.certificateFiles = [publicTlsCertificate];

    # Stub beets library DB so cratedigger-web can open it read-only.
    environment.etc."cratedigger/beets.db" = {
      text = "";
      mode = "0644";
    };

    services.cratedigger = {
      enable = true;
      src = cratediggerSrc;
      user = "cratedigger";
      group = "beets-library";
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
      # Stranger posture (U10/R12): beets validation ON — the full
      # rendered-config surface (config.ini beets keys + config.yaml) is
      # what a real first boot produces.
      beets.validation = {
        enable = true;
        stagingDir = "/var/lib/cratedigger-music/Incoming";
        # Keep the tracking parent distinct from staging so the sandbox
        # contract proves it is derived from its own option.
        trackingFile = "/var/lib/cratedigger-music/Re-download/tracking.jsonl";
      };
      beets.package = {
        discogsTokenFile = "/etc/cratedigger/discogs-token";
        discogsOperatorGroup = "cratedigger-ops";
      };
      # Keep the library root separate from the default DB parent. The module
      # must create its sibling-of-stateDir default without granting writes to
      # the music root.
      beets.config = {
        directory = "/var/lib/cratedigger-music/Beets";
      };
      web = {
        enable = true;
        hostName = "music.vm.test";
        gatewayPort = 18086;
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
      # Exercise the configured branch of Jellyfin's targeted refresh option.
      # The Python config/notifier tests separately pin the null -> full-library
      # fallback.
      notifiers.jellyfin.libraryId = "music-library-item-id";
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
    specialisation.cratedigger-basic-alternate.configuration = {
      services.cratedigger.web = {
        basicAuthFile = lib.mkForce
          "/run/cratedigger-test-auth/basic-alternate.htpasswd";
        enableInsecure = lib.mkForce false;
      };
    };

    environment.systemPackages = [deployHoldTool metadataGateTool];

    # Simulate a downstream metadata gate holding every application unit.
    # Only the independent renderer may materialise runtime configuration on
    # first boot; the test removes this hold before exercising the apps.
    systemd.tmpfiles.rules = [
      "d /var/lib/cratedigger-music 0777 root root -"
      "d /var/lib/cratedigger-music/Beets 2775 cratedigger beets-library -"
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
        serviceConfig.ExecCondition = [configHoldGate];
      }))
      {
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
        # The probe is test-only and ordered after the module's config render.
        cratedigger-importer.serviceConfig.ExecStartPre =
          lib.mkAfter [importerSandboxProbe];
        # Preview and YouTube retain stateDir for their established workflows,
        # but the sibling DB parent must remain unreachable in both sandboxes.
        cratedigger-import-preview-worker.serviceConfig.ExecStartPre =
          lib.mkAfter [stateDbDenialProbe];
        cratedigger-youtube-ingest.serviceConfig.ExecStartPre =
          lib.mkAfter [stateDbDenialProbe];
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
    # cratedigger-db-migrate's after/requires on postgresql.service when
    # createLocally is set, and every app unit requires the migrate unit —
    # transitively serialising first boot behind PostgreSQL.

    # Speed up the VM
    virtualisation.memorySize = 2048;
  };

  testScript = ''
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

    # A deploy must materialise the new runtime config independently of every
    # application unit. Downstream consumers can intentionally gate those
    # units with ExecCondition; systemd evaluates that before ExecStartPre, so
    # an app-owned renderer leaves stale mutable config throughout an outage.
    machine.wait_for_unit("cratedigger-test-config-hold.service")
    machine.succeed("test -f /run/cratedigger-test-config-hold")
    machine.wait_for_unit("cratedigger-config-render.service")
    state = machine.succeed("systemctl is-active cratedigger-config-render.service").strip()
    assert state == "active", f"config renderer unit not active: {state}"
    machine.succeed("test -f /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^host_url = http://192.0.2.21:5030$' /var/lib/cratedigger/config.ini")

    # Re-rendering on a config-only deploy must neither remove nor recreate the
    # main pipeline's active singleton lock. Pin both the fresh config and lock
    # preservation across an explicit renderer restart.
    machine.succeed("printf 'active-cycle\\n' > /var/lib/cratedigger/.cratedigger.lock")
    machine.succeed("sed -i 's#http://192.0.2.21:5030#http://stale.invalid#' /var/lib/cratedigger/config.ini")
    machine.succeed("before=$(stat -c '%d:%i' /var/lib/cratedigger/.cratedigger.lock); systemctl restart cratedigger-config-render.service; after=$(stat -c '%d:%i' /var/lib/cratedigger/.cratedigger.lock); test \"$before\" = \"$after\"")
    machine.succeed("grep -q '^host_url = http://192.0.2.21:5030$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -qx 'active-cycle' /var/lib/cratedigger/.cratedigger.lock")
    # Long-running workers may restart when their unit changes, but their
    # fallback is render-only. Only the timer-owned main service may clear the
    # pipeline lock.
    machine.succeed("systemctl cat cratedigger-importer.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-import-preview-worker.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-unfindable.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-youtube-ingest.service | grep -q cratedigger-render-config")
    machine.succeed("systemctl cat cratedigger-web.service | grep -q cratedigger-render-config")
    machine.fail("systemctl cat cratedigger-importer.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-import-preview-worker.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-unfindable.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-youtube-ingest.service | grep -q cratedigger-pipeline-prestart")
    machine.fail("systemctl cat cratedigger-web.service | grep -q cratedigger-pipeline-prestart")
    machine.succeed("systemctl cat cratedigger.service | grep -q cratedigger-pipeline-prestart")

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

    acquire_status, acquire_output = machine.execute("timeout 60 cratedigger-deploy-hold acquire")
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

    # Qualify idempotent post-switch verification and the staged release. The
    # config ExecCondition keeps the controlled VM cycle cheap; PR1 owns real
    # invocation capture/verification in the deploy workflow.
    machine.succeed("rm -r /run/systemd/system/cratedigger-metadata-gate-watchdog.service.d")
    machine.succeed("systemctl daemon-reload")
    machine.succeed("cratedigger-deploy-hold verify-held")
    machine.succeed("rm /run/cratedigger-deploy-hold-blocker")
    machine.wait_until_succeeds("systemctl show cratedigger-deploy-hold-blocker.service --property=ActiveState --value | grep -qx inactive")
    machine.succeed("cratedigger-deploy-hold prepare-controlled")
    machine.succeed("cratedigger-deploy-hold open-main-timer")
    machine.succeed("cratedigger-deploy-hold finish-release aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.succeed("cratedigger-deploy-hold complete aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    machine.fail("test -e /run/cratedigger-deploy-hold")
    machine.fail("test -e /run/cratedigger-metadata-gate/holds/manual")
    for timer in (
        "cratedigger.timer",
        "cratedigger-unfindable.timer",
        "cratedigger-metadata-gate-watchdog.timer",
    ):
        machine.fail(f"test -e /run/systemd/system.control/{timer}")
        state = machine.succeed(f"systemctl show {timer} --property=LoadState --value").strip()
        assert state == "loaded", f"{timer} not restored after release: {state}"

    # Starting the main service remains safe: its idempotent pre-start render is
    # retained as a fallback and clears the test's deliberately stale lock. It
    # will fail because there is no real slskd.
    machine.succeed("rm /run/cratedigger-test-config-hold")
    machine.succeed("systemctl start --no-block cratedigger.service")
    machine.wait_until_succeeds("test ! -f /var/lib/cratedigger/.cratedigger.lock")
    machine.succeed(
        "systemctl kill --kill-whom=all --signal=SIGKILL cratedigger.service || true"
    )
    machine.succeed("systemctl reset-failed cratedigger.service || true")
    machine.succeed("systemctl start cratedigger-importer.service cratedigger-import-preview-worker.service cratedigger-youtube-ingest.service cratedigger-web.service")
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
            "--property=ReadWritePaths"
        )
        return dict(line.split("=", 1) for line in out.splitlines())

    def _assert_sandbox_properties(unit, properties, expected_paths):
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

    def _assert_sandbox_contract(unit, expected_paths):
        _assert_sandbox_properties(unit, _unit_properties(unit), expected_paths)
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
                "/var/lib/cratedigger-music/Beets",
                "/var/lib/cratedigger-beets-db",
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
                "/var/lib/cratedigger-music/Beets",
                "/var/lib/cratedigger-beets-db",
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
        "/var/lib/cratedigger-music/Beets",
        "/var/lib/cratedigger-beets-db",
        "/var/lib/cratedigger-music/Incoming",
    ])
    _assert_sandbox_contract("cratedigger-importer.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
        "/var/lib/cratedigger-music/Beets",
        "/var/lib/cratedigger-beets-db",
        "/var/lib/cratedigger-music/Incoming",
        "/var/lib/cratedigger-music/Re-download",
    ])
    _assert_sandbox_contract("cratedigger-import-preview-worker.service", [
        "/var/lib/cratedigger",
        "/var/lib/cratedigger/processing",
        "/var/lib/cratedigger-downloads",
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

    # The importer probe ran inside the unit's sandbox. These pins prove every
    # configured authority root was writable while unrelated world-writable
    # locations remained effectively read-only despite their Unix modes.
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger-beets-db)\" = cratedigger:beets-library:2775")
    machine.succeed("runuser -u cratedigger -- test -w /var/lib/cratedigger-beets-db")
    machine.succeed("test \"$(stat -c %a /var/lib/cratedigger-music/unrelated)\" = 777")
    machine.fail("test -e /var/lib/cratedigger-music/unrelated/escape")
    machine.succeed("test \"$(stat -c %a /var/lib/cratedigger-world-writable)\" = 777")
    machine.fail("test -e /var/lib/cratedigger-world-writable/escape")
    machine.succeed("test -s /var/lib/cratedigger/processing/sandbox-probe/tone.mp3")
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
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing)\" = cratedigger:beets-library:700")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing/albums)\" = cratedigger:beets-library:700")
    machine.succeed("test \"$(stat -c %U:%G:%a /var/lib/cratedigger/processing/preview)\" = cratedigger:beets-library:700")
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
    machine.succeed("awk '$0 == \"[Paths]\" { in_paths=1; next } in_paths && /^\\[/ { exit } in_paths { print }' /var/lib/cratedigger/config.ini | grep -qx 'processing_dir = /var/lib/cratedigger/processing'")
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
    machine.succeed("grep -q 'api_key_file = /etc/cratedigger/slskd-api-key' /var/lib/cratedigger/config.ini")
    # The secret itself must NEVER appear in config.ini — that's the whole fix.
    machine.fail("grep -q 'test-api-key-do-not-use' /var/lib/cratedigger/config.ini")
    # config.ini is now world-readable since it contains no secrets.
    mode = machine.succeed("stat -c %a /var/lib/cratedigger/config.ini").strip()
    assert mode == "644", f"config.ini should be 0644, got {mode}"
    machine.succeed("grep -q 'enabled = True' /var/lib/cratedigger/config.ini")  # beets validation ON (stranger posture)
    machine.succeed("grep -q '\\[Quality Ranks\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.transparent = 192$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.excellent = 160$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.good = 112$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^vorbis.acceptable = 96$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.transparent = 320$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.excellent = 256$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.good = 192$' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^wma.acceptable = 128$' /var/lib/cratedigger/config.ini")
    # U5 (tier-2): the module renders the beets runtime keys so every
    # beets subprocess resolves the pinned interpreter + rendered config.
    machine.succeed("grep -q 'config_dir = /var/lib/cratedigger/beets' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^library = /var/lib/cratedigger-beets-db/beets-library.db$' /var/lib/cratedigger/config.ini")
    machine.succeed("test -d /var/lib/cratedigger-beets-db")
    machine.succeed("test -f /var/lib/cratedigger-beets-db/.sandbox-probe")
    machine.succeed("grep -q 'python = /nix/store/' /var/lib/cratedigger/config.ini")
    # U6 (tier-2): one MB value, rendered for the python consumers too.
    machine.succeed("grep -q 'api_base = https://musicbrainz.org' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '\\[Peer Cache\\]' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'redis_host = 127.0.0.1' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q 'ttl_seconds = 604800' /var/lib/cratedigger/config.ini")
    machine.succeed("grep -q '^library_id = music-library-item-id$' /var/lib/cratedigger/config.ini")
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
    machine.succeed("grep -q 'dsn = postgresql:///cratedigger?host=/run/postgresql' /var/lib/cratedigger/config.ini")
    # (password_file *keys* are fine — they are the #117 *File pattern;
    # what must not exist is an actual credential value.)
    machine.fail("grep -Eqi 'password *= *[^ ]|pgpassword' /var/lib/cratedigger/config.ini")
    machine.succeed(
        "systemctl show cratedigger-db-migrate -p Environment"
        " | grep -q 'PIPELINE_DB_DSN=postgresql:///cratedigger?host=/run/postgresql'"
    )

    # Module-owned first-boot ordering (U7/U10): migrate is serialised
    # behind PostgreSQL; every app unit requires migrate — the stranger's
    # first boot cannot race the database.
    machine.succeed("systemctl show -p After cratedigger-db-migrate.service | grep -q postgresql.service")
    machine.succeed("systemctl show -p Requires cratedigger-db-migrate.service | grep -q postgresql.service")

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
        "expected=$(printf '%s\\n' '127.0.0.1:18086' '[::1]:18086' | sort); "
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
        "-p Group --value)\" = beets-library; "
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
        "= beets-library"
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
    machine.succeed(
        "pid=$(systemctl show cratedigger-web.service -p MainPID --value); "
        "! tr '\\0' '\\n' < /proc/$pid/cmdline | grep -F '${headerRecorder}'; "
        "tr '\\0' '\\n' < /proc/$pid/cmdline "
        "| grep -A1 -Fx -- '--canonical-origin' "
        "| tail -n1 | grep -Fx 'https://music.vm.test'"
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
    machine.succeed(f"test -x {basic_system}/bin/switch-to-configuration")
    machine.succeed(f"test -x {insecure_system}/bin/switch-to-configuration")
    machine.succeed(
        f"test -x {alternate_basic_system}/bin/switch-to-configuration"
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

    def _switch_web_system(system_path, label):
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
            "-w %{http_code} -H Host:music.vm.test "
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
            "'127.0.0.1:18086' '[::1]:18086' | sort); "
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

    # U3+U4 (tier-2): cratedigger owns the beet runtime AND its config.
    # The module rendered config.yaml into BEETSDIR during ExecStartPre
    # (the `systemctl start cratedigger.service` above); cratedigger-beet
    # resolves it and loads the FULL production plugin set with an included
    # Discogs token readable by the explicit operator group.
    machine.succeed("command -v cratedigger-beet")
    machine.succeed("test -f /var/lib/cratedigger/beets/config.yaml")
    mode = machine.succeed("stat -c %a /var/lib/cratedigger/beets/config.yaml").strip()
    assert mode == "644", f"config.yaml should be 0644, got {mode}"
    machine.succeed("test -f /var/lib/cratedigger/beets/secrets.yaml")
    secret_mode = machine.succeed("stat -c %a /var/lib/cratedigger/beets/secrets.yaml").strip()
    secret_group = machine.succeed("stat -c %G /var/lib/cratedigger/beets/secrets.yaml").strip()
    assert secret_mode == "440", f"secrets.yaml should be 0440, got {secret_mode}"
    assert secret_group == "cratedigger-ops", secret_group
    machine.succeed("sudo -u beets-operator test -r /var/lib/cratedigger/beets/secrets.yaml")
    machine.fail("sudo -u unrelated-user test -r /var/lib/cratedigger/beets/secrets.yaml")

    # Semantic assertions on the rendered YAML (duplicate_keys nesting,
    # plugin list, public-MB defaults, included token).
    machine.succeed("${pyWithYaml}/bin/python3 ${checkRenderedBeetsConfig}")

    version_out = machine.succeed("sudo -u cratedigger cratedigger-beet version")
    plugins_line = next(
        line for line in version_out.splitlines() if line.startswith("plugins:")
    )
    loaded = {p.strip() for p in plugins_line.split(":", 1)[1].split(",")}
    for plugin in (
        "musicbrainz discogs fetchart embedart lyrics lastgenre scrub "
        "info missing duplicates edit fromfilename ftintitle the inline "
        "permissions"
    ).split():
        assert plugin in loaded, f"plugin {plugin} not loaded: {version_out}"
    operator_version = machine.succeed("sudo -u beets-operator cratedigger-beet version")
    operator_plugins = next(
        line for line in operator_version.splitlines() if line.startswith("plugins:")
    )
    assert operator_plugins == plugins_line, (operator_plugins, plugins_line)
    machine.succeed("sudo -u beets-operator cratedigger-beet config > /dev/null")
    service_groups = machine.succeed("id -nG cratedigger").split()
    assert "cratedigger-ops" in service_groups, service_groups

    # Execute a real 12-track removal through the actual module-rendered
    # config as an authorized non-root operator. This crosses renderer,
    # include permissions, every shipped plugin, and pinned Beets itself.
    beets_python = machine.succeed(
        "sed -n 's/^python = //p' /var/lib/cratedigger/config.ini"
    ).strip()
    seed_out = machine.succeed(
        f"sudo -u cratedigger env BEETSDIR=/var/lib/cratedigger/beets "
        f"{beets_python} ${beetsDestructiveFixture} seed"
    )
    child_album_id = int(seed_out.strip().split("=", 1)[1])
    remove_out = machine.succeed(
        "sudo -u beets-operator cratedigger-beet -P importsource "
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
        f"sudo -u beets-operator env BEETSDIR=/var/lib/cratedigger/beets "
        f"{beets_python} ${cratediggerSrc}/harness/delete_album.py "
        "2>/tmp/exact-delete.stderr"
    )
    child_payload = json.loads(child_out)
    assert child_payload["status"] == "completed", child_payload
    assert json.dumps(child_payload, separators=(",", ":")) == child_out
    machine.succeed("test ! -s /tmp/exact-delete.stderr")
    machine.succeed(
        f"sudo -u cratedigger env BEETSDIR=/var/lib/cratedigger/beets "
        f"{beets_python} ${beetsDestructiveFixture} verify"
    )
  '';
}
