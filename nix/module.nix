# Upstream NixOS module for Cratedigger.
#
# Generic, paths-as-options, no sops/homelab/nspawn assumptions. Downstream
# wrappers (e.g. ~/nixosconfig) layer their secrets backend, DB host, and
# reverse-proxy on top via standard NixOS option merging.
#
# Identity defaults to the dedicated Cratedigger account. Deployments retain
# control of its supplementary groups and external Beets/slskd permissions.
{
  config,
  lib,
  pkgs,
  ...
}: let
  inherit (lib) mkOption mkEnableOption mkIf optional optionalString types concatStringsSep;

  cfg = config.services.cratedigger;
  src = cfg.src;
  # stateDir is mutable Cratedigger state, distinct from the immutable
  # deployment-owned runtime configuration rendered below.
  canonicalStateDir = cfg.stateDir;
  isAbsoluteNormalizedPath = path:
    path != null
    && lib.hasPrefix "/" path
    && (path == "/" || !lib.hasSuffix "/" path)
    && !lib.hasInfix "//" path
    && !lib.hasInfix "/./" path
    && !lib.hasSuffix "/." path
    && !lib.hasInfix "/../" path
    && !lib.hasSuffix "/.." path;
  canonicalStateDirIsValid =
    canonicalStateDir != "/" && isAbsoluteNormalizedPath canonicalStateDir;
  # Every unit/wrapper interpolates the DSN; guard it so a missing value
  # yields the actionable message even if string coercion is forced before
  # the module assertions run. createLocally mkDefaults this option to the
  # local unix socket (peer auth as cfg.user — no password material, KTD5).
  pipelineDsn =
    if cfg.pipelineDb.dsn != null
    then cfg.pipelineDb.dsn
    else throw "services.cratedigger.pipelineDb.dsn is not set: either set it to your PostgreSQL connection string, or set services.cratedigger.pipelineDb.createLocally = true to provision a local database.";

  # The deployment owns Beets. Cratedigger only consumes the exact supplied
  # Python package and effective configuration authority.
  beetsPackage = cfg.beets.runtime.package;

  # Build every application and checker from the supplied Beets package.
  # The assertion below requires that package to belong to packageSet.python3.
  cratedigger = cfg.packageSet.callPackage ./package.nix { inherit beetsPackage; };
  pythonEnv = cratedigger.pythonEnv;

  pyRunner = "${pythonEnv}/bin/python";

  runtimePath = lib.makeBinPath [
    pkgs.bash
    pkgs.coreutils
    pkgs.gnugrep
    pkgs.gnused
    pkgs.curl
    pkgs.jq
    pkgs.ffmpeg
    pkgs.mp3val
    pkgs.flac
    pkgs.sox
  ];
  redisServiceUnits = optional cfg.redis.enable "redis-cratedigger.service";
  webRuntimeDirectory = "/run/cratedigger-web";
  webSocketPath = "${webRuntimeDirectory}/web.sock";
  webHostName =
    if cfg.web.hostName != null
    then cfg.web.hostName
    else "invalid.invalid";
  webHostLabels = lib.splitString "." webHostName;
  webHostLabelIsValid = label: let
    length = builtins.stringLength label;
  in
    length >= 1
    && length <= 63
    && builtins.match "([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9-]*[A-Za-z0-9])" label != null;
  webHostNameIsValid =
    cfg.web.hostName != null
    && webHostName == lib.toLower webHostName
    && builtins.stringLength webHostName <= 253
    && lib.all webHostLabelIsValid webHostLabels
    && builtins.match "[0-9.]+" webHostName == null;
  webBasicEnabled = cfg.web.basicAuthFile != null;
  # External authorization is a whole-site allow/deny decision owned by a
  # component in front of this gateway. The module never calls that component:
  # it neither proxies to it nor probes it, so a probe result can never be
  # mistaken for an authorization guarantee. The mode exists so the deployment
  # stops asserting that authentication is absent.
  webExternalEnabled = cfg.web.externalAuth;
  webModeCount = lib.count (enabled: enabled) [
    webBasicEnabled
    cfg.web.enableInsecure
    webExternalEnabled
  ];
  webBasicAuthConfiguredPath =
    if cfg.web.basicAuthFile != null
    then cfg.web.basicAuthFile
    else "/invalid/cratedigger.htpasswd";
  webGatewayMode =
    if webBasicEnabled
    then "basic"
    else if webExternalEnabled
    then "external"
    else "insecure";
  webGatewayPolicyIdentity =
    if webBasicEnabled
    then "basic:${webBasicAuthConfiguredPath}"
    else webGatewayMode;
  webGatewayPolicyFingerprint =
    builtins.hashString "sha256" webGatewayPolicyIdentity;
  webGatewayActiveMarker =
    "${webRuntimeDirectory}/gateway-policy-${webGatewayPolicyFingerprint}";
  webGatewayPolicyFile = "/etc/cratedigger/web-gateway-policy";
  webGatewayReloadReceipt =
    "${webRuntimeDirectory}/gateway-reload-receipt";
  webGatewayPolicyText = ''
    format=1
    gateway_mode=${webGatewayMode}
    gateway_credential_path=${
      if webBasicEnabled then webBasicAuthConfiguredPath else "-"
    }
    gateway_marker_path=${webGatewayActiveMarker}
  '';
  webBasicAuthPathSegments =
    lib.drop 1 (lib.splitString "/" webBasicAuthConfiguredPath);
  webBasicAuthPathIsValid =
    cfg.web.basicAuthFile != null
    && lib.hasPrefix "/" cfg.web.basicAuthFile
    && !lib.hasSuffix "/" cfg.web.basicAuthFile
    && lib.all
      (segment:
        segment != "."
        && segment != ".."
        && builtins.match "[A-Za-z0-9._+-]+" segment != null)
      webBasicAuthPathSegments
    && cfg.web.basicAuthFile != "/nix/store"
    && !lib.hasPrefix "/nix/store/" cfg.web.basicAuthFile;
  webGatewayListen = map
    (addr: {
      inherit addr;
      port = cfg.web.gatewayPort;
    })
    cfg.web.gatewayAddresses;
  webProxyRequestConfig = ''
    proxy_http_version 1.1;
    proxy_pass_request_headers off;
    proxy_set_header Host ${webHostName};
    proxy_set_header Connection "";
    proxy_set_header Content-Length ''$content_length;
    proxy_set_header Content-Type ''$content_type;
    proxy_set_header Accept ''$http_accept;
    proxy_set_header Range ''$http_range;
    proxy_set_header Origin ''$http_origin;
    proxy_set_header Referer ''$http_referer;
    proxy_set_header X-Cratedigger-Request-Channel browser;
  '';
  webHealthProxyRequestConfig = ''
    # The anonymous exception is bodyless and single-request by construction.
    # Do not let a health request body become a second request on the upstream
    # BaseHTTPRequestHandler connection.
    proxy_http_version 1.0;
    proxy_pass_request_headers off;
    proxy_pass_request_body off;
    proxy_set_header Host ${webHostName};
    proxy_set_header Connection close;
    proxy_set_header Content-Length "";
    proxy_set_header Transfer-Encoding "";
  '';
  webResourceIsolationConfig = ''
    add_header Content-Security-Policy "frame-ancestors 'none'" always;
    add_header X-Frame-Options "DENY" always;
    add_header Cross-Origin-Resource-Policy "same-origin" always;
  '';
  webBasicAuthValidationScript = pkgs.writeShellScript "cratedigger-web-basic-auth-validate" ''
    set -euo pipefail
    set -f

    configured_path="''${1:-}"
    nginx_user=${lib.escapeShellArg config.services.nginx.user}
    nginx_group=${lib.escapeShellArg config.services.nginx.group}
    application_user=${lib.escapeShellArg cfg.user}
    access_group=${lib.escapeShellArg cfg.web.accessGroup}

    fail() {
      echo "Cratedigger Basic authentication validation failed: $*" >&2
      exit 1
    }

    ${pkgs.coreutils}/bin/test -n "$configured_path" \
      || fail "configured credential path is empty"
    resolved_path="$(${pkgs.coreutils}/bin/realpath -e -- "$configured_path")" \
      || fail "configured credential path does not resolve"
    case "$resolved_path" in
      /nix/store|/nix/store/*)
        fail "resolved credential target is inside /nix/store"
        ;;
    esac

    ${pkgs.coreutils}/bin/test -f "$resolved_path" \
      || fail "resolved credential target is not a regular file"
    ${pkgs.coreutils}/bin/test -s "$resolved_path" \
      || fail "resolved credential target is empty"
    auth_metadata="$(${pkgs.coreutils}/bin/stat -Lc '%u:%G:%a' -- "$resolved_path")"
    ${pkgs.coreutils}/bin/test "$auth_metadata" = "0:$nginx_group:440" \
      || fail "resolved credential target must be root:$nginx_group mode 0440"
    target_acl="$(${pkgs.acl}/bin/getfacl -cp -- "$resolved_path")" \
      || fail "cannot inspect resolved credential ACL"
    expected_target_acl=$'user::r--\ngroup::r--\nother::---'
    ${pkgs.coreutils}/bin/test "$target_acl" = "$expected_target_acl" \
      || fail "resolved credential target must have only the base 0440 ACL"

    run_as() {
      local user="$1"
      shift
      ${pkgs.util-linux}/bin/runuser -u "$user" -- "$@"
    }

    run_as "$nginx_user" ${pkgs.coreutils}/bin/test -r "$configured_path" \
      || fail "nginx cannot read the configured credential path"

    group_record="$(${pkgs.getent}/bin/getent group "$access_group")" \
      || fail "socket access group does not exist"
    IFS=: read -r _ _ access_gid supplementary_members <<< "$group_record"
    ${pkgs.coreutils}/bin/test -n "$access_gid" \
      || fail "socket access group has no gid"

    restricted_users="$application_user"
    old_ifs="$IFS"
    IFS=,
    for member in $supplementary_members; do
      if ${pkgs.coreutils}/bin/test -n "$member" \
        && ${pkgs.coreutils}/bin/test "$member" != "$nginx_user"; then
        restricted_users="$restricted_users $member"
      fi
    done
    IFS="$old_ifs"
    while IFS=: read -r candidate _ _ primary_gid _; do
      if ${pkgs.coreutils}/bin/test "$primary_gid" = "$access_gid" \
        && ${pkgs.coreutils}/bin/test "$candidate" != "$nginx_user"; then
        restricted_users="$restricted_users $candidate"
      fi
    done < <(${pkgs.getent}/bin/getent passwd)

    for user in $restricted_users; do
      ${pkgs.getent}/bin/getent passwd "$user" >/dev/null \
        || fail "socket-authorized identity does not exist"
      if run_as "$user" ${pkgs.coreutils}/bin/test -r "$configured_path"; then
        fail "application or non-nginx socket identity can read the credential"
      fi
    done

    check_ancestors() {
      local path="$1"
      local directory
      local directory_acl
      local directory_acl_lines
      local directory_mode
      local owner_uid
      directory="$(${pkgs.coreutils}/bin/dirname -- "$path")"
      while true; do
        owner_uid="$(${pkgs.coreutils}/bin/stat -Lc '%u' -- "$directory")" \
          || fail "credential ancestor is unavailable"
        ${pkgs.coreutils}/bin/test "$owner_uid" = "0" \
          || fail "credential ancestors must be root-owned"
        directory_mode="$(${pkgs.coreutils}/bin/stat -Lc '%a' -- "$directory")"
        if (( (8#$directory_mode & 0022) != 0 )); then
          fail "credential ancestors must not be group/other writable"
        fi
        directory_acl="$(${pkgs.acl}/bin/getfacl -cp -- "$directory")" \
          || fail "cannot inspect credential ancestor ACL"
        directory_acl_lines="$(
          ${pkgs.coreutils}/bin/printf '%s\n' "$directory_acl" \
            | ${pkgs.gnugrep}/bin/grep -c '^'
        )"
        ${pkgs.coreutils}/bin/test "$directory_acl_lines" = "3" \
          || fail "credential ancestors must not have extended/default ACLs"
        ${pkgs.coreutils}/bin/printf '%s\n' "$directory_acl" \
          | ${pkgs.gnugrep}/bin/grep -Eq '^user::[r-][w-][x-]$' \
          || fail "credential ancestor owner ACL is malformed"
        ${pkgs.coreutils}/bin/printf '%s\n' "$directory_acl" \
          | ${pkgs.gnugrep}/bin/grep -Eq '^group::[r-][w-][x-]$' \
          || fail "credential ancestor group ACL is malformed"
        ${pkgs.coreutils}/bin/printf '%s\n' "$directory_acl" \
          | ${pkgs.gnugrep}/bin/grep -Eq '^other::[r-][w-][x-]$' \
          || fail "credential ancestor other ACL is malformed"
        for user in "$nginx_user" $restricted_users; do
          if run_as "$user" ${pkgs.coreutils}/bin/test -w "$directory"; then
            fail "credential ancestor is writable by a credential consumer"
          fi
        done
        ${pkgs.coreutils}/bin/test "$directory" = "/" && break
        directory="$(${pkgs.coreutils}/bin/dirname -- "$directory")"
      done
    }

    check_ancestors "$configured_path"
    if ${pkgs.coreutils}/bin/test "$resolved_path" != "$configured_path"; then
      check_ancestors "$resolved_path"
    fi
  '';
  webApplicationCredentialIsolationScript = pkgs.writeShellScript
    "cratedigger-web-basic-auth-app-isolation"
    ''
      set -euo pipefail

      # This command deliberately runs without systemd's "+" privilege
      # prefix, under the final User/Group/SupplementaryGroups merged onto
      # cratedigger-web.service. It therefore catches downstream identity
      # overrides and numeric supplementary GIDs that a Nix name comparison
      # cannot reliably model.
      if ${pkgs.coreutils}/bin/test \
        -r ${lib.escapeShellArg webBasicAuthConfiguredPath}; then
        echo "Cratedigger Basic authentication isolation failed: " \
          "the web application can read its gateway credential" >&2
        exit 1
      fi
    '';
  webNginxEffectiveIdentityScript = pkgs.writeShellScript
    "cratedigger-web-nginx-effective-identity"
    ''
      set -euo pipefail
      set -f

      # This command deliberately has no systemd "+" privilege prefix. It
      # therefore observes the final User/Group/SupplementaryGroups merged
      # onto nginx.service, including numeric group IDs and downstream
      # overrides that cannot be reconstructed safely from Nix names alone.
      expected_user=${lib.escapeShellArg config.services.nginx.user}
      expected_group=${lib.escapeShellArg config.services.nginx.group}
      access_group=${lib.escapeShellArg cfg.web.accessGroup}
      forbidden_groups=(
        ${lib.concatMapStringsSep "\n        " lib.escapeShellArg webForbiddenAuthorityGroups}
      )

      identity_fail() {
        echo "Cratedigger nginx effective identity validation failed: $*" >&2
        exit 1
      }

      expected_passwd="$(
        ${pkgs.getent}/bin/getent passwd "$expected_user"
      )" || identity_fail "services.nginx.user does not resolve"
      IFS=: read -r _ _ expected_uid _ _ _ _ <<< "$expected_passwd"
      expected_group_record="$(
        ${pkgs.getent}/bin/getent group "$expected_group"
      )" || identity_fail "services.nginx.group does not resolve"
      IFS=: read -r _ _ expected_gid _ <<< "$expected_group_record"
      access_group_record="$(
        ${pkgs.getent}/bin/getent group "$access_group"
      )" || identity_fail "services.cratedigger.web.accessGroup does not resolve"
      IFS=: read -r _ _ access_gid _ <<< "$access_group_record"

      effective_uid="$(${pkgs.coreutils}/bin/id -u)"
      effective_gid="$(${pkgs.coreutils}/bin/id -g)"
      effective_gids="$(${pkgs.coreutils}/bin/id -G)"
      ${pkgs.coreutils}/bin/test "$effective_uid" != 0 \
        || identity_fail "effective nginx UID must not be 0"
      ${pkgs.coreutils}/bin/test "$effective_gid" != 0 \
        || identity_fail "effective nginx primary GID must not be 0"
      ${pkgs.coreutils}/bin/test "$effective_uid" = "$expected_uid" \
        || identity_fail \
          "effective UID differs from services.nginx.user"
      ${pkgs.coreutils}/bin/test "$effective_gid" = "$expected_gid" \
        || identity_fail \
          "effective primary GID differs from services.nginx.group"

      has_access_group=false
      for member_gid in $effective_gids; do
        if ${pkgs.coreutils}/bin/test "$member_gid" = "$access_gid"; then
          has_access_group=true
          break
        fi
      done
      ${pkgs.coreutils}/bin/test "$has_access_group" = true \
        || identity_fail \
          "effective nginx group set lacks required accessGroup GID $access_gid"

      for forbidden_group in "''${forbidden_groups[@]}"; do
        if forbidden_group_record="$(
          ${pkgs.getent}/bin/getent group "$forbidden_group"
        )"; then
          IFS=: read -r _ _ forbidden_gid _ <<< "$forbidden_group_record"
          for member_gid in $effective_gids; do
            if ${pkgs.coreutils}/bin/test \
              "$member_gid" = "$forbidden_gid"; then
              identity_fail \
                "effective nginx group set contains forbidden $forbidden_group GID $forbidden_gid"
            fi
          done
        fi
      done
    '';
  webGatewayClearMarkers = ''
    ${pkgs.findutils}/bin/find \
      ${lib.escapeShellArg webRuntimeDirectory} \
      -maxdepth 1 \
      -type f \
      -name ${lib.escapeShellArg "gateway-policy-*"} \
      -delete
  '';
  webGatewayStartClearScript = pkgs.writeShellScript
    "cratedigger-web-gateway-clear-start"
    ''
      set -euo pipefail

      # Clear readiness as root before the unprivileged effective-identity
      # preflight. If that check fails, no stale marker may survive and imply
      # that the rejected nginx identity is ready to serve the gateway.
      ${webGatewayClearMarkers}
      ${pkgs.coreutils}/bin/rm -f -- \
        ${lib.escapeShellArg webGatewayReloadReceipt}
    '';
  webGatewayReadPolicy = ''
    gateway_fail() {
      echo "Cratedigger web gateway policy validation failed: $*" >&2
      exit 1
    }

    gateway_mode=
    gateway_credential_path=
    gateway_marker_path=
    gateway_policy_sha256=
    policy_lines=()
    mapfile -t policy_lines < ${lib.escapeShellArg webGatewayPolicyFile} \
      || gateway_fail "cannot read the policy descriptor"
    ${pkgs.coreutils}/bin/test "''${#policy_lines[@]}" = 4 \
      || gateway_fail "policy descriptor must contain exactly four lines"
    ${pkgs.coreutils}/bin/test "''${policy_lines[0]}" = "format=1" \
      || gateway_fail "policy descriptor has an unsupported format"
    case "''${policy_lines[1]}" in
      gateway_mode=*) gateway_mode="''${policy_lines[1]#gateway_mode=}" ;;
      *) gateway_fail "policy descriptor is missing gateway_mode" ;;
    esac
    case "''${policy_lines[2]}" in
      gateway_credential_path=*)
        gateway_credential_path="''${policy_lines[2]#gateway_credential_path=}"
        ;;
      *) gateway_fail "policy descriptor is missing gateway_credential_path" ;;
    esac
    case "''${policy_lines[3]}" in
      gateway_marker_path=*)
        gateway_marker_path="''${policy_lines[3]#gateway_marker_path=}"
        ;;
      *) gateway_fail "policy descriptor is missing gateway_marker_path" ;;
    esac
    case "$gateway_mode" in
      basic)
        ${pkgs.coreutils}/bin/printf '%s\n' "$gateway_credential_path" \
          | ${pkgs.gnugrep}/bin/grep -Eq \
            '^/[A-Za-z0-9._+-]+(/[A-Za-z0-9._+-]+)*$' \
          || gateway_fail "Basic policy has an invalid credential path"
        ;;
      insecure)
        ${pkgs.coreutils}/bin/test "$gateway_credential_path" = "-" \
          || gateway_fail "insecure policy must not name a credential"
        ;;
      external)
        ${pkgs.coreutils}/bin/test "$gateway_credential_path" = "-" \
          || gateway_fail "external policy must not name a credential"
        ;;
      *)
        gateway_fail "policy descriptor has an invalid mode"
        ;;
    esac
    ${pkgs.coreutils}/bin/printf '%s\n' "$gateway_marker_path" \
      | ${pkgs.gnugrep}/bin/grep -Eq \
        '^/run/cratedigger-web/gateway-policy-[0-9a-f]{64}$' \
      || gateway_fail "policy descriptor has an invalid marker path"
    gateway_policy_sha256="$(
      ${pkgs.coreutils}/bin/sha256sum \
        ${lib.escapeShellArg webGatewayPolicyFile} \
        | ${pkgs.coreutils}/bin/cut -d ' ' -f 1
    )"
  '';
  webGatewayAssertPolicyUnchanged = ''
    current_policy_sha256="$(
      ${pkgs.coreutils}/bin/sha256sum \
        ${lib.escapeShellArg webGatewayPolicyFile} \
        | ${pkgs.coreutils}/bin/cut -d ' ' -f 1
    )"
    ${pkgs.coreutils}/bin/test \
      "$current_policy_sha256" = "$gateway_policy_sha256" \
      || gateway_fail "policy descriptor changed during validation"
  '';
  webGatewayFingerprintCredential = ''
    if ${pkgs.coreutils}/bin/test "$gateway_mode" = basic; then
      gateway_credential_sha256="$(
        ${pkgs.coreutils}/bin/sha256sum -- "$gateway_credential_path" \
          | ${pkgs.coreutils}/bin/cut -d ' ' -f 1
      )"
    else
      gateway_credential_sha256=-
    fi
  '';
  webGatewayWriteReloadReceipt = ''
    receipt_temp="$(
      ${pkgs.coreutils}/bin/mktemp \
        ${lib.escapeShellArg "${webGatewayReloadReceipt}.XXXXXX"}
    )"
    trap '${pkgs.coreutils}/bin/rm -f -- "$receipt_temp"' EXIT
    ${pkgs.coreutils}/bin/chown root:root "$receipt_temp"
    ${pkgs.coreutils}/bin/chmod 0600 "$receipt_temp"
    ${pkgs.coreutils}/bin/printf \
      '%s\n' \
      "format=1" \
      "policy_sha256=$gateway_policy_sha256" \
      "gateway_mode=$gateway_mode" \
      "gateway_credential_path=$gateway_credential_path" \
      "gateway_credential_sha256=$gateway_credential_sha256" \
      "gateway_marker_path=$gateway_marker_path" \
      > "$receipt_temp"
    ${pkgs.coreutils}/bin/mv -T \
      "$receipt_temp" \
      ${lib.escapeShellArg webGatewayReloadReceipt}
    trap - EXIT
  '';
  webGatewayReadReloadReceipt = ''
    receipt_lines=()
    mapfile -t receipt_lines < ${lib.escapeShellArg webGatewayReloadReceipt} \
      || gateway_fail "cannot read the reload receipt"
    ${pkgs.coreutils}/bin/test "''${#receipt_lines[@]}" = 6 \
      || gateway_fail "reload receipt must contain exactly six lines"
    ${pkgs.coreutils}/bin/test "''${receipt_lines[0]}" = "format=1" \
      || gateway_fail "reload receipt has an unsupported format"
    case "''${receipt_lines[1]}" in
      policy_sha256=*)
        receipt_policy_sha256="''${receipt_lines[1]#policy_sha256=}"
        ;;
      *) gateway_fail "reload receipt is missing policy_sha256" ;;
    esac
    case "''${receipt_lines[2]}" in
      gateway_mode=*)
        receipt_gateway_mode="''${receipt_lines[2]#gateway_mode=}"
        ;;
      *) gateway_fail "reload receipt is missing gateway_mode" ;;
    esac
    case "''${receipt_lines[3]}" in
      gateway_credential_path=*)
        receipt_gateway_credential_path="''${receipt_lines[3]#gateway_credential_path=}"
        ;;
      *) gateway_fail "reload receipt is missing gateway_credential_path" ;;
    esac
    case "''${receipt_lines[4]}" in
      gateway_credential_sha256=*)
        receipt_gateway_credential_sha256="''${receipt_lines[4]#gateway_credential_sha256=}"
        ;;
      *) gateway_fail "reload receipt is missing credential fingerprint" ;;
    esac
    case "''${receipt_lines[5]}" in
      gateway_marker_path=*)
        receipt_gateway_marker_path="''${receipt_lines[5]#gateway_marker_path=}"
        ;;
      *) gateway_fail "reload receipt is missing gateway_marker_path" ;;
    esac
    ${pkgs.coreutils}/bin/printf '%s\n' "$receipt_policy_sha256" \
      | ${pkgs.gnugrep}/bin/grep -Eq '^[0-9a-f]{64}$' \
      || gateway_fail "reload receipt has an invalid policy fingerprint"
    case "$receipt_gateway_credential_sha256" in
      -) ;;
      *)
        ${pkgs.coreutils}/bin/printf \
          '%s\n' "$receipt_gateway_credential_sha256" \
          | ${pkgs.gnugrep}/bin/grep -Eq '^[0-9a-f]{64}$' \
          || gateway_fail \
            "reload receipt has an invalid credential fingerprint"
        ;;
    esac
  '';
  webGatewayPublishMarker = ''
    ${pkgs.coreutils}/bin/install \
      -m 0440 \
      -o root \
      -g ${lib.escapeShellArg cfg.web.accessGroup} \
      /dev/null \
      "$gateway_marker_path"
  '';
  webGatewayStartScript = pkgs.writeShellScript "cratedigger-web-gateway-start" ''
    set -euo pipefail

    ${webGatewayClearMarkers}
    ${pkgs.coreutils}/bin/rm -f -- \
      ${lib.escapeShellArg webGatewayReloadReceipt}
    ${webGatewayReadPolicy}
    if ${pkgs.coreutils}/bin/test "$gateway_mode" = basic; then
      ${webBasicAuthValidationScript} "$gateway_credential_path"
    fi
    ${webGatewayAssertPolicyUnchanged}
    ${webGatewayPublishMarker}
  '';
  webGatewayReloadPrepareScript = pkgs.writeShellScript "cratedigger-web-gateway-prepare-reload" ''
    set -euo pipefail

    # Remove every Cratedigger gateway policy marker before validating. Old
    # workers keep checking their own policy-specific marker, so publishing
    # the new policy after HUP cannot reopen a stale authentication policy.
    ${webGatewayClearMarkers}
    ${pkgs.coreutils}/bin/rm -f -- \
      ${lib.escapeShellArg webGatewayReloadReceipt}
    ${webGatewayReadPolicy}
    if ${pkgs.coreutils}/bin/test "$gateway_mode" = basic; then
      ${webBasicAuthValidationScript} "$gateway_credential_path"
    fi
    ${webGatewayFingerprintCredential}
    ${webGatewayAssertPolicyUnchanged}
    ${webGatewayWriteReloadReceipt}
  '';
  webGatewayReloadFinishScript = pkgs.writeShellScript "cratedigger-web-gateway-finish-reload" ''
    set -euo pipefail

    reload_receipt=${lib.escapeShellArg webGatewayReloadReceipt}
    trap '${pkgs.coreutils}/bin/rm -f -- "$reload_receipt"' EXIT
    ${webGatewayReadPolicy}
    ${webGatewayReadReloadReceipt}
    ${pkgs.coreutils}/bin/test \
      "$gateway_policy_sha256" = "$receipt_policy_sha256" \
      || gateway_fail "policy descriptor differs from the validated receipt"
    ${pkgs.coreutils}/bin/test \
      "$gateway_mode" = "$receipt_gateway_mode" \
      || gateway_fail "policy mode differs from the validated receipt"
    ${pkgs.coreutils}/bin/test \
      "$gateway_credential_path" = "$receipt_gateway_credential_path" \
      || gateway_fail \
        "credential path differs from the validated receipt"
    ${pkgs.coreutils}/bin/test \
      "$gateway_marker_path" = "$receipt_gateway_marker_path" \
      || gateway_fail "marker path differs from the validated receipt"
    if ${pkgs.coreutils}/bin/test "$gateway_mode" = basic; then
      ${webBasicAuthValidationScript} "$gateway_credential_path"
    fi
    ${webGatewayFingerprintCredential}
    ${pkgs.coreutils}/bin/test \
      "$gateway_credential_sha256" \
      = "$receipt_gateway_credential_sha256" \
      || gateway_fail "credential changed after reload validation"
    ${webGatewayAssertPolicyUnchanged}
    # systemd runs this only after nginx's config test and HUP both succeed.
    # Publish only the receipt-bound policy marker when the current descriptor
    # and credential remain byte-identical to what reload preparation
    # validated. Any overlap or invalid replacement leaves every marker absent.
    gateway_marker_path="$receipt_gateway_marker_path"
    ${webGatewayPublishMarker}
    ${pkgs.coreutils}/bin/rm -f -- \
      ${lib.escapeShellArg webGatewayReloadReceipt}
    trap - EXIT
  '';
  webNginxUserExtraGroups =
    config.users.users.${config.services.nginx.user}.extraGroups or [];
  webNginxServiceSupplementaryGroups =
    config.systemd.services.nginx.serviceConfig.SupplementaryGroups or [];
  webNginxServiceUser =
    config.systemd.services.nginx.serviceConfig.User or null;
  webNginxServiceGroup =
    config.systemd.services.nginx.serviceConfig.Group or null;
  webApplicationServiceSupplementaryGroups =
    config.systemd.services.cratedigger-web.serviceConfig.SupplementaryGroups
      or [];
  webApplicationServiceUser =
    config.systemd.services.cratedigger-web.serviceConfig.User or null;
  webApplicationServiceGroup =
    config.systemd.services.cratedigger-web.serviceConfig.Group or null;
  webNginxReverseMemberGroups = lib.mapAttrsToList
    (name: group: group.name or name)
    (
      lib.filterAttrs
        (_: group: lib.elem config.services.nginx.user (group.members or []))
        config.users.groups
    );
  webNginxDeclaredSupplementaryGroups = lib.unique (
    webNginxUserExtraGroups
    ++ webNginxServiceSupplementaryGroups
    ++ webNginxReverseMemberGroups
  );
  # Known high-authority groups must never double as the web socket boundary.
  # Arbitrary group purpose cannot be inferred from its name, so consumers
  # still own keeping any other configured accessGroup dedicated.
  webForbiddenAuthorityGroupKeys = lib.unique [
    cfg.group
    "root"
    "wheel"
    "cratedigger-ops"
    "users"
  ];
  webForbiddenAuthorityGroups = lib.unique (
    map
      (
        group:
          let
            groupConfig = config.users.groups.${group} or {};
          in
            groupConfig.name or group
      )
      webForbiddenAuthorityGroupKeys
  );
  webForbiddenAuthorityGroupIds = lib.unique (
    lib.concatMap
      (
        group:
          let
            groupConfig = config.users.groups.${group} or {};
            groupGid = groupConfig.gid or null;
          in
            optional (groupGid != null) (toString groupGid)
      )
      webForbiddenAuthorityGroupKeys
  );
  webNginxForbiddenGroupOverlap = lib.intersectLists
    (webForbiddenAuthorityGroups ++ webForbiddenAuthorityGroupIds)
    webNginxDeclaredSupplementaryGroups;
  webAccessGroupConfig = config.users.groups.${cfg.web.accessGroup} or {};
  webAccessGroupGid = webAccessGroupConfig.gid or null;
  webAccessGroupNamesAndIds =
    [
      cfg.web.accessGroup
      (webAccessGroupConfig.name or cfg.web.accessGroup)
    ]
    ++ optional
      (webAccessGroupGid != null)
      (toString webAccessGroupGid);
  webNginxHasAccessGroup =
    lib.intersectLists
      (lib.unique webAccessGroupNamesAndIds)
      webNginxDeclaredSupplementaryGroups
    != [];
  webNginxServiceIdentityIsModuleOwned =
    webNginxServiceUser == config.services.nginx.user
    && webNginxServiceGroup == config.services.nginx.group;
  webNginxConfiguredUser =
    config.users.users.${config.services.nginx.user} or {};
  webNginxConfiguredGroup =
    config.users.groups.${config.services.nginx.group} or {};
  webNginxConfiguredUid = webNginxConfiguredUser.uid or null;
  webNginxConfiguredPrimaryGid = webNginxConfiguredGroup.gid or null;
  webNginxUserIsSafe =
    config.services.nginx.user != "root"
    && (
      webNginxConfiguredUid == null
      || webNginxConfiguredUid != 0
    );
  webNginxPrimaryGroupIsSafe =
    !lib.elem config.services.nginx.group webForbiddenAuthorityGroups
    && (
      webNginxConfiguredPrimaryGid == null
      || !lib.elem
        (toString webNginxConfiguredPrimaryGid)
        webForbiddenAuthorityGroupIds
    );
  webAccessGroupIsSafe =
    cfg.web.accessGroup != config.services.nginx.group
    && !lib.elem cfg.web.accessGroup webForbiddenAuthorityGroups;
  webApplicationCredentialGroupIsSafe =
    !lib.elem
      config.services.nginx.group
      webApplicationServiceSupplementaryGroups;
  webApplicationServiceIdentityIsModuleOwned =
    webApplicationServiceUser == cfg.user
    && webApplicationServiceGroup == cfg.group;
  applicationConfiguredUser = config.users.users.${cfg.user} or {};
  applicationConfiguredGroup = config.users.groups.${cfg.group} or {};
  applicationIdentityIsNamed = value:
    builtins.match "[0-9]+" value == null;
  applicationUserIsNonRoot =
    cfg.user != "root"
    && applicationIdentityIsNamed cfg.user
    && (applicationConfiguredUser.uid or null) != 0;
  applicationGroupIsNonRoot =
    cfg.group != "root"
    && applicationIdentityIsNamed cfg.group
    && (applicationConfiguredGroup.gid or null) != 0;

  # CD-SEC-04: these are the only long-running units that accept untrusted
  # network/media input. Keep the systemd hardening literal and shared; each
  # unit below supplies only the filesystem roots its workflow mutates.
  untrustedInputSandbox = writePaths: {
    NoNewPrivileges = true;
    PrivateTmp = true;
    ProtectSystem = "strict";
    ProtectHome = true;
    RestrictAddressFamilies = ["AF_UNIX" "AF_INET" "AF_INET6"];
    SystemCallFilter = ["@system-service"];
    ReadWritePaths = lib.unique writePaths;
  };

  slskdWritePaths = optional (cfg.slskd.downloadDir != null) cfg.slskd.downloadDir;
  validationStagingWritePaths = optional
    (cfg.beets.validation.stagingDir != null)
    cfg.beets.validation.stagingDir;
  validationTrackingWritePaths = optional
    (cfg.beets.validation.trackingFile != null)
    (dirOf cfg.beets.validation.trackingFile);
  beetsReadinessUnits = cfg.beets.runtime.readinessUnits;
  presentExternalPath = path: optional (path != null) path;
  missingOkExternalPath = path: map (value: "-${value}") (presentExternalPath path);
  beetsConfigReadOnlyPaths = missingOkExternalPath cfg.beets.runtime.configDir;
  beetsObserverReadOnlyPaths = beetsConfigReadOnlyPaths ++
    missingOkExternalPath cfg.beets.runtime.expectedStateFile;
  beetsLibraryAuthorityRoots = lib.unique (
    presentExternalPath cfg.beets.runtime.expectedDirectory
    ++ optional
      (cfg.beets.runtime.expectedLibrary != null)
      (dirOf cfg.beets.runtime.expectedLibrary)
  );
  beetsMainReadOnlyPaths = beetsObserverReadOnlyPaths
    ++ map (path: "-${path}") beetsLibraryAuthorityRoots;
  beetsMutationWritePaths =
    map (path: "-${path}") beetsLibraryAuthorityRoots;
  # Issue #1176 PR3: the web, importer, and preview-worker units ALL run
  # with ProtectHome + PrivateTmp (untrustedInputSandbox); a
  # localImport.dir under /home, /tmp, or /var/tmp would otherwise resolve
  # every candidate as missing the moment ANY of the three tries to read
  # it (predicted by PR2's own `localImport.dir` description, which named
  # this as PR3's obligation) — the web unit is the lane's actual front
  # door (enqueue_local_import opens the root and the candidate path
  # before a job is even enqueued; a review round found it missing from
  # this bind set while the two downstream workers were not, so the front
  # door itself was sandbox-blind). Bound read-only, never writable — the
  # operator's folder is strictly read-only input this lane may copy
  # from, never mutate. `missingOkExternalPath` already degrades to `[]`
  # when `dir` is null.
  localImportReadOnlyPaths = lib.optionals cfg.localImport.enable
    (missingOkExternalPath cfg.localImport.dir);
  webSandboxWritePaths = [
    cfg.stateDir
    cfg.processingDir
  ] ++ slskdWritePaths ++ beetsMutationWritePaths ++ validationStagingWritePaths;
  importerSandboxWritePaths = [
    cfg.stateDir
    cfg.processingDir
  ] ++ slskdWritePaths ++ beetsMutationWritePaths ++ validationStagingWritePaths
    ++ validationTrackingWritePaths
    ++ missingOkExternalPath cfg.beets.runtime.expectedStateFile;
  previewWorkerSandboxWritePaths = [
    cfg.stateDir
    cfg.processingDir
  ] ++ slskdWritePaths;
  youtubeIngestSandboxWritePaths = [
    cfg.stateDir
    cfg.youtubeIngest.tempDir
  ] ++ validationStagingWritePaths;
  beetsRuntimeEnvironment = ''
    export BEETSDIR="${cfg.beets.runtime.configDir}"
    export CRATEDIGGER_RUNTIME_CONFIG="${configTemplate}"
  '';

  # CLI wrappers — the only place PYTHONPATH is set.
  cratediggerPkg = pkgs.writeShellScriptBin "cratedigger" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    exec ${pyRunner} ${src}/cratedigger.py \
      --redis-host "${cfg.redis.host}" \
      --redis-port ${toString cfg.redis.port} \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  # PYTHONPATH carries ONLY the repo root. Adding ${src}/lib or ${src}/web
  # puts our modules at the top level of sys.path, where lib/beets.py
  # shadows the beets PyPI package for any subprocess (including `beet`)
  # that inherits PYTHONPATH. That shadow load executes our `import
  # msgspec` before the subprocess can reach its own site-packages, and
  # crashes it with ModuleNotFoundError. All internal imports use
  # `from lib.X import Y` / `from web.X import Y` against the repo root
  # already, so the flat entries are both unnecessary and harmful.
  # pipeline-cli is a package (scripts/pipeline_cli/, issue #495). Python's
  # -c mode normally prepends the current directory to sys.path; -P removes
  # that hostile shadow-package input. The trusted Nix source remains first
  # in PYTHONPATH ahead of any explicitly inherited operator additions.
  pipelineCli = pkgs.writeShellScriptBin "pipeline-cli" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pythonEnv}/bin/python -P -c \
      'from scripts.pipeline_cli.cli import main; main(api_socket="${webSocketPath}")' \
      --dsn "${pipelineDsn}" \
      "$@"
  '';

  pipelineMigrate = pkgs.writeShellScriptBin "pipeline-migrate" ''
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pythonEnv}/bin/python ${src}/scripts/migrate_db.py \
      --dsn "${pipelineDsn}" \
      --migrations-dir "${src}/migrations" "$@"
  '';

  decisionDifferential = pkgs.writeShellScriptBin "decision-differential" ''
    # Read-only developer instrumentation.  Pin both interpreter and source
    # to this deployed module generation; never depend on an operator checkout.
    # The script inserts its own exact repository root, so isolate from every
    # inherited Python import/startup/user-site influence.
    unset PYTHONPATH PYTHONHOME PYTHONSTARTUP
    export PYTHONNOUSERSITE=1
    exec ${pythonEnv}/bin/python -I ${src}/scripts/decision_differential.py "$@"
  '';

  importerPkg = pkgs.writeShellScriptBin "cratedigger-importer" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/importer.py \
      --dsn "${pipelineDsn}" \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  previewWorkerPkg = pkgs.writeShellScriptBin "cratedigger-import-preview-worker" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/import_preview_worker.py \
      --dsn "${pipelineDsn}" \
      --workers ${toString cfg.importer.previewWorkers} \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  webPkg = pkgs.writeShellScriptBin "cratedigger-web" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    # MB/Discogs API bases are NOT passed here (issue #497): the immutable config's
    # [MusicBrainz]/[Discogs] api_base is the ONE production source, read at
    # startup via configure_api_bases_from_runtime_config(). The
    # --mb-api/--discogs-api flags still exist on web/server.py for
    # dev-only overrides for a manual `cratedigger-web` invocation — the module
    # deliberately stops passing them so there is no second path to keep
    # in sync with that runtime authority.
    exec ${pyRunner} ${src}/web/server.py \
      --canonical-origin "https://${webHostName}" \
      ${optionalString cfg.web.enableInsecure "--insecure-mode"} \
      ${optionalString cfg.web.externalAuth "--external-auth-mode"} \
      --dsn "${pipelineDsn}" \
      --redis-host "${cfg.web.redis.host}" \
      --redis-port ${toString cfg.web.redis.port} \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  checkBeetsConfigPkg = pkgs.writeShellScriptBin "cratedigger-check-beets-config" ''
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}"
    exec ${pyRunner} ${src}/scripts/check_beets_config.py \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  # YouTube-rescue ingest drainer — see scripts/youtube_ingest_worker.py.
  # Worker-specific PATH: pkgs.yt-dlp is prepended so the worker's
  # `shutil.which("yt-dlp")` resolves. It is deliberately NOT added to
  # `runtimePath` (which is shared across the rest of the cratedigger
  # units) because no other service needs yt-dlp and we want a single
  # boundary owning the binary lookup. The worker runs `yt-dlp` via
  # subprocess and inherits this PATH from the wrapper.
  youtubeIngestWorkerPkg = pkgs.writeShellScriptBin "cratedigger-youtube-ingest" ''
    export PATH="${pkgs.yt-dlp}/bin:${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/youtube_ingest_worker.py \
      --dsn "${pipelineDsn}" \
      --temp-dir "${cfg.youtubeIngest.tempDir}" \
      --staging-dir "${toString cfg.beets.validation.stagingDir}" \
      --poll-interval ${toString cfg.youtubeIngest.pollIntervalSeconds} \
      ${optionalString (cfg.youtubeIngest.sourceAddress != "") ''--source-address "${cfg.youtubeIngest.sourceAddress}" ''}"$@"
  '';

  # Unfindable detection oneshot — see lib/unfindable_detection_service.py.
  # Runs in its own process so the R20 cadence-never-changes invariant
  # is structurally enforceable at the systemd level: this binary has
  # no way to reach the regular 5-min plan loop's cursor mutators.
  unfindableDetectionPkg = pkgs.writeShellScriptBin "cratedigger-unfindable" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/run_unfindable_detection.py \
      --dsn "${pipelineDsn}" "$@"
  '';

  # Daily whole-library retag-divergence census oneshot (#1142) — see
  # lib/retag_divergence_audit.py and lib/retag_divergence_census_snapshot.py.
  # Runs the UNBOUNDED whole-library census (~93,700 files / ~200s
  # measured live) and atomically publishes a snapshot into cfg.stateDir;
  # the dashboard route reads that persisted snapshot, never a fresh
  # scan. Beets-only — no pipeline DB dependency, unlike the unfindable
  # detection oneshot above.
  retagDivergenceCensusPkg = pkgs.writeShellScriptBin "cratedigger-retag-census" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/run_retag_divergence_census.py \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  # Daily whole-library source/catalog/files completeness census (#1149).
  # Separate from both the five-minute pipeline and retag census: it reads
  # mirrors, Beets and files, then atomically publishes its own snapshot.
  libraryCompletenessCensusPkg = pkgs.writeShellScriptBin "cratedigger-library-completeness-census" ''
    export PATH="${runtimePath}:$PATH"
    ${beetsRuntimeEnvironment}
    export PYTHONPATH="${src}''${PYTHONPATH:+:$PYTHONPATH}"
    exec ${pyRunner} ${src}/scripts/run_library_completeness_census.py \
      "$@" \
      --config "${configTemplate}" \
      --runtime-dir "${cfg.stateDir}"
  '';

  # [Quality Ranks] section — declarative mirror of QualityRankConfig.defaults().
  # Pinned by TestQualityRankConfigDefaults in tests/test_quality_decisions.py.
  qualityRanksSection = let
    qr = cfg.qualityRanks;
    bandSection = codecKey: bands: ''
      ${codecKey}.transparent = ${toString bands.transparent}
      ${codecKey}.excellent = ${toString bands.excellent}
      ${codecKey}.good = ${toString bands.good}
      ${codecKey}.acceptable = ${toString bands.acceptable}
    '';
  in
    lib.strings.removeSuffix "\n" ''
      [Quality Ranks]
      bitrate_metric = ${qr.bitrateMetric}
      within_rank_tolerance_kbps = ${toString qr.withinRankToleranceKbps}

      ${bandSection "opus" qr.bands.opus}
      ${bandSection "mp3" qr.bands.mp3}
      ${bandSection "aac" qr.bands.aac}
      ${bandSection "vorbis" qr.bands.vorbis}
      ${bandSection "wma" qr.bands.wma}
    '';

  # Runtime secrets live at the *File paths referenced here. The Cratedigger
  # Python code reads them on demand via CratediggerConfig.resolved_*() accessors,
  # so nothing sensitive is embedded in this immutable Nix-store config.
  configTemplate = pkgs.writeText "cratedigger-config.ini" ''
    [Slskd]
    api_key_file = ${toString cfg.slskd.apiKeyFile}
    host_url = ${cfg.slskd.hostUrl}
    url_base = ${cfg.slskd.urlBase}
    download_dir = ${toString cfg.slskd.downloadDir}
    delete_searches = ${if cfg.slskd.deleteSearches then "True" else "False"}
    stalled_timeout = ${toString cfg.slskd.stalledTimeout}
    remote_queue_timeout = ${toString cfg.slskd.remoteQueueTimeout}

    [Paths]
    processing_dir = ${cfg.processingDir}

    [Local Import]
    enabled = ${if cfg.localImport.enable then "True" else "False"}
    dir = ${if cfg.localImport.dir != null then toString cfg.localImport.dir else ""}

    [Release Settings]
    use_most_common_tracknum = ${if cfg.releaseSettings.useMostCommonTracknum then "True" else "False"}
    allow_multi_disc = ${if cfg.releaseSettings.allowMultiDisc then "True" else "False"}
    accepted_countries = ${concatStringsSep "," cfg.releaseSettings.acceptedCountries}
    skip_region_check = ${if cfg.releaseSettings.skipRegionCheck then "True" else "False"}
    accepted_formats = ${concatStringsSep "," cfg.releaseSettings.acceptedFormats}

    [Search Settings]
    search_timeout = ${toString cfg.searchSettings.searchTimeout}
    maximum_peer_queue = ${toString cfg.searchSettings.maximumPeerQueue}
    minimum_peer_upload_speed = ${toString cfg.searchSettings.minimumPeerUploadSpeed}
    minimum_filename_match_ratio = ${toString cfg.searchSettings.minimumFilenameMatchRatio}
    allowed_filetypes = ${concatStringsSep "," cfg.searchSettings.allowedFiletypes}
    ignored_users = ${concatStringsSep "," cfg.searchSettings.ignoredUsers}
    search_for_tracks = ${if cfg.searchSettings.searchForTracks then "True" else "False"}
    album_prepend_artist = ${if cfg.searchSettings.albumPrependArtist then "True" else "False"}
    track_prepend_artist = ${if cfg.searchSettings.trackPrependArtist then "True" else "False"}
    search_type = ${cfg.searchSettings.searchType}
    parallel_searches = ${toString cfg.searchSettings.parallelSearches}
    number_of_albums_to_grab = ${toString cfg.searchSettings.numberOfAlbumsToGrab}
    title_blacklist = ${concatStringsSep "," cfg.searchSettings.titleBlacklist}
    search_blacklist = ${concatStringsSep "," cfg.searchSettings.searchBlacklist}
    search_response_limit = ${toString cfg.searchSettings.searchResponseLimit}
    search_file_limit = ${toString cfg.searchSettings.searchFileLimit}
    browse_top_k = ${toString cfg.searchSettings.browseTopK}
    browse_global_max_workers = ${toString cfg.searchSettings.browseGlobalMaxWorkers}
    search_max_inflight = ${toString cfg.searchSettings.searchMaxInflight}

    [Download Settings]
    download_filtering = ${if cfg.downloadSettings.downloadFiltering then "True" else "False"}
    use_extension_whitelist = ${if cfg.downloadSettings.useExtensionWhitelist then "True" else "False"}
    extensions_whitelist = ${concatStringsSep "," cfg.downloadSettings.extensionsWhitelist}

    [Beets]
    directory = ${cfg.beets.runtime.expectedDirectory}
    library = ${cfg.beets.runtime.expectedLibrary}
    config_dir = ${cfg.beets.runtime.configDir}
    state_file = ${cfg.beets.runtime.expectedStateFile}
    python = ${pythonEnv}/bin/python
    secret_include = ${cfg.beets.runtime.expectedSecretInclude}

    [Beets Validation]
    enabled = ${if cfg.beets.validation.enable then "True" else "False"}
    harness_path = ${cfg.beets.validation.harnessPath}
    distance_threshold = ${toString cfg.beets.validation.distanceThreshold}
    staging_dir = ${toString cfg.beets.validation.stagingDir}
    tracking_file = ${toString cfg.beets.validation.trackingFile}
    verified_lossless_target = ${cfg.beets.validation.verifiedLosslessTarget}

    [MusicBrainz]
    api_base = ${cfg.musicbrainz.apiBase}

    [Discogs]
    api_base = ${if cfg.discogs.apiBase != null then cfg.discogs.apiBase else ""}

    ${qualityRanksSection}
    [Pipeline DB]
    enabled = ${if cfg.pipelineDb.enable then "True" else "False"}
    dsn = ${pipelineDsn}

    [Peer Cache]
    redis_host = ${cfg.redis.host}
    redis_port = ${toString cfg.redis.port}
    ttl_seconds = ${toString cfg.peerCache.ttlSeconds}
    speed_ttl_seconds = ${toString cfg.peerCache.speedTtlSeconds}
    redis_connect_timeout_ms = ${toString cfg.peerCache.redisConnectTimeoutMs}
    redis_operation_timeout_ms = ${toString cfg.peerCache.redisOperationTimeoutMs}

    [Plex]
    url = ${cfg.notifiers.plex.url}
    token_file = ${toString cfg.notifiers.plex.tokenFile}
    library_section_id = ${toString cfg.notifiers.plex.librarySectionId}
    path_map = ${cfg.notifiers.plex.pathMap}

    [Jellyfin]
    url = ${cfg.notifiers.jellyfin.url}
    token_file = ${toString cfg.notifiers.jellyfin.tokenFile}
    path_map = ${cfg.notifiers.jellyfin.pathMap}

    [Logging]
    level = ${cfg.logging.level}
    format = ${cfg.logging.format}
    datefmt = ${cfg.logging.datefmt}
  '';

  # The main pipeline alone owns its singleton lock cleanup. Runtime and Beets
  # configuration are immutable deployment inputs and are never rendered here.
  pipelinePreStartScript = pkgs.writeShellScript "cratedigger-pipeline-prestart" ''
    set -euo pipefail
    rm -f "${cfg.stateDir}/.cratedigger.lock"
  '';

  # Optional health check for a stuck slskd reconnect loop. Generic — the
  # restart command is configurable so non-systemd slskd setups still work.
  slskdHealthCheck = pkgs.writeShellScript "cratedigger-slskd-healthcheck" ''
    set -euo pipefail
    api_key=$(${pkgs.coreutils}/bin/cat "${toString cfg.slskd.apiKeyFile}")
    status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
    connected=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isConnected // false')
    logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
    if [ "$connected" = "true" ] && [ "$logged_in" = "true" ]; then
      exit 0
    fi
    echo "cratedigger: slskd not connected (connected=$connected, loggedIn=$logged_in)" >&2
    ${optionalString (cfg.healthCheck.onFailureCommand != "") ''
      echo "cratedigger: running onFailureCommand to recover slskd..." >&2
      ${cfg.healthCheck.onFailureCommand}
      for i in $(${pkgs.coreutils}/bin/seq 1 12); do
        ${pkgs.coreutils}/bin/sleep 5
        status=$(${pkgs.curl}/bin/curl -sf -H "X-API-Key: $api_key" "${cfg.slskd.hostUrl}/api/v0/server" 2>/dev/null || echo '{}')
        logged_in=$(echo "$status" | ${pkgs.jq}/bin/jq -r '.isLoggedIn // false')
        if [ "$logged_in" = "true" ]; then
          echo "cratedigger: slskd reconnected after recovery" >&2
          exit 0
        fi
      done
    ''}
    echo "cratedigger: slskd unhealthy, skipping run" >&2
    exit 1
  '';

  # `mkCodecBands` is the same factory the legacy module used.
  mkCodecBands = codec: defaults: {
    transparent = mkOption {
      type = types.int;
      default = defaults.transparent;
      description = "${codec} TRANSPARENT rank floor (kbps).";
    };
    excellent = mkOption {
      type = types.int;
      default = defaults.excellent;
      description = "${codec} EXCELLENT rank floor (kbps).";
    };
    good = mkOption {
      type = types.int;
      default = defaults.good;
      description = "${codec} GOOD rank floor (kbps).";
    };
    acceptable = mkOption {
      type = types.int;
      default = defaults.acceptable;
      description = "${codec} ACCEPTABLE rank floor (kbps).";
    };
  };
in {
  options.services.cratedigger = {
    enable = mkEnableOption "Cratedigger — Soulseek download pipeline";

    src = mkOption {
      type = types.path;
      default = ../.;
      defaultText = lib.literalExpression "../.";
      description = "Path to the cratedigger source tree. Defaults to this flake's repo root.";
    };

    packageSet = mkOption {
      type = types.pkgs;
      default = pkgs;
      defaultText = lib.literalExpression "pkgs";
      description = ''
        Package set used to build cratedigger's runtime closure (the python
        env, and from it the pinned beets). When this module is imported via
        the flake's `nixosModules.default`, this is pinned to the nixpkgs
        from cratedigger's own flake.lock — the rev the test suite and the
        real-beets contract test ran against. Setting it explicitly is the
        deliberate escape hatch for consumers who refuse the second nixpkgs
        evaluation; doing so forfeits the tested-closure guarantee (your
        beets/python may drift from what cratedigger's suite verified).
      '';
    };

    user = mkOption {
      type = types.str;
      default = "cratedigger";
      description = ''
        Dedicated non-root UNIX user to run Cratedigger as. Configure its
        surrounding permissions (slskd group membership, external Beets DB and
        library access, /Incoming write access, etc.) through normal NixOS
        account options. Root is rejected for the guarded application identity.
      '';
    };

    group = mkOption {
      type = types.str;
      default = "cratedigger";
      description = "UNIX group to run cratedigger as. See `user` for context.";
    };

    stateDir = mkOption {
      type = types.str;
      default = "/var/lib/cratedigger";
      description = "Mutable runtime state directory (lock and operational state). Must be an absolute normalized non-root path without a trailing slash.";
    };

    processingDir = mkOption {
      type = types.str;
      default = "${cfg.stateDir}/processing";
      defaultText = lib.literalExpression ''"''${cfg.stateDir}/processing"'';
      description = ''
        Private Cratedigger-owned root for materialized albums and preview
        snapshots.  Must be an absolute normalized path (no trailing slash,
        no . or .. components, no doubled slashes), disjoint from
        slskd.downloadDir, and beneath a parent that is not writable by
        slskd or another group.
      '';
    };

    # Manual local-import lane (issue #1176): an operator names a request
    # ID and a directory already on disk, and Cratedigger COPIES it into
    # private processing scratch (never importing in place, never mutating
    # or deleting the operator's folder) before running it through the
    # existing preview -> importer -> quality gate -> beets chain. PR2
    # shipped the configuration surface and the execution-time path
    # authority (lib.fs_authority.open_configured_local_import_directory);
    # PR3 wires it up end to end — pipeline-cli import-local / POST
    # /api/pipeline/import-local, the local_import preview/import lanes,
    # and the sandbox bind (localImportReadOnlyPaths, below) all THREE
    # sandboxed units (web, importer, preview-worker) need to read this
    # option's configured `dir`.
    #
    # Both options are deliberately redundant (enable AND dir, rather than
    # a single nullable dir) because it reads more obviously to a human,
    # and dir deliberately has NO working default even when enabled: naming
    # this directory is a conscious operator act, and an installation that
    # never sets it has NO local-import surface at all. A build that failed
    # until a directory was named would just push operators to type a
    # placeholder like /tmp, manufacturing the very surface this option
    # exists to gate.
    localImport = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Enable the manual local-import lane. Off by default with no
          working `localImport.dir` default — see `localImport.dir` for why
          enabling this is a conscious, two-part act.
        '';
      };
      dir = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          Absolute, normalized root directory (no trailing slash, no `.`
          or `..` components) the local-import lane is permitted to read
          from. Deliberately has NO default, even when
          `localImport.enable = true`: naming this directory is a
          conscious operator act, and an installation that does not set it
          has no local-import surface at all. Must not be `/`.

          Execution-time authority
          (`lib.fs_authority.open_configured_local_import_directory`)
          re-checks every one of these constraints itself rather than
          trusting this module-time assertion (a preflight is not
          authority) and additionally refuses any candidate that resolves
          inside a Cratedigger-owned subtree — the WHOLE `processingDir`
          (not just its `albums/` child: the private `preview/` scratch is
          equally off-limits), the Beets validation staging directory, the
          slskd download directory, and the Beets library root — even when
          it is nested under this root. A broad root such as `/mnt/virtio`
          can legitimately contain those trees too, so that narrowing
          lives at execution time, not as a module-level assertion here
          (which would have to reject the whole broad root outright).

          The web, importer, and preview-worker units ALL run with
          `ProtectHome = true` AND `PrivateTmp = true` (this module's
          shared `untrustedInputSandbox`), which makes `/home` empty and
          gives `/tmp`/`/var/tmp` their own private, per-unit namespaces
          inside every one of those units. A `dir` under any of `/home`,
          `/tmp`, or `/var/tmp` — natural choices for this lane, `/tmp`
          especially, since it is the exact placeholder this option's own
          no-default design exists to discourage — would otherwise resolve
          every candidate as missing the moment ANY of the three tries to
          read it: the web unit is the lane's actual front door
          (`enqueue_local_import`, called directly from
          `web/routes/pipeline_mutations.py`, opens the root AND the
          candidate path before a job is even enqueued — the CLI subcommand
          is a pure HTTP relay to that same route, per CLI/API symmetry,
          and never opens the path itself), and the importer/preview-worker
          units each re-open it later during execution. PR3 (issue #1176)
          closes that gap for all three: every one of them read-only-binds
          this exact `dir` (`localImportReadOnlyPaths`, the same
          missing-tolerant `-`-prefixed shape every other external bind in
          this module uses) so it is visible inside the sandbox regardless
          of which of `/home`, `/tmp`, `/var/tmp`, or an ordinary path it
          names — never writable: the operator's folder is strictly
          read-only input this lane may copy from, never mutate. A PR3
          review round found the web unit missing from this bind set — the
          front door was sandbox-blind while the two downstream workers
          were not, so a `dir` under any of those roots failed at
          enqueue-time with a 422/503 about a folder that plainly exists.
        '';
      };
    };

    timer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run cratedigger periodically via systemd timer.";
      };
      onBootSec = mkOption {
        type = types.str;
        default = "5min";
        description = "Delay after boot before first timer fire.";
      };
      onUnitInactiveSec = mkOption {
        type = types.str;
        default = "1s";
        description = "Delay after each completed cycle before starting the next one.";
      };
    };

    importer = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Run the long-lived importer worker that drains the shared import queue.";
      };
      previewWorkers = mkOption {
        type = types.int;
        default = 2;
        description = "Number of async import preview workers to run before the serial importer lane.";
      };
    };

    # YouTube-rescue ingest worker. Drains album_requests rows the operator
    # has marked for YouTube fallback (`pipeline-cli youtube-rescue <id>` or
    # POST /api/pipeline/<id>/youtube-rescue), invoking `yt-dlp` to stage
    # audio into the configured beets-validation staging directory's
    # auto-import child for the existing importer worker to pick up. The
    # unit is defined here but `enable` defaults to `false` so
    # the in-flake module ships dormant — the downstream NixOS wrapper at
    # ~/nixosconfig/modules/nixos/services/cratedigger.nix is the right
    # layer to flip this on and layer on network-namespace hardening
    # (`serviceConfig.NetworkNamespacePath`, `BindReadOnlyPaths`, etc.).
    youtubeIngest = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Run the long-lived YouTube-rescue ingest worker
          (cratedigger-youtube-ingest.service). Requires `yt-dlp` on the
          worker's PATH — the unit's wrapper prepends `${pkgs.yt-dlp}/bin`
          for this unit only, never on the shared runtime PATH.
        '';
      };
      tempDir = mkOption {
        type = types.str;
        default = "${cfg.stateDir}/youtube-ingest-temp";
        defaultText = lib.literalExpression ''"''${cfg.stateDir}/youtube-ingest-temp"'';
        description = ''
          Per-process scratch directory yt-dlp downloads into before files
          are moved to the configured auto-import staging directory. Created by systemd-tmpfiles
          with the same ownership as the cratedigger user.
        '';
      };
      pollIntervalSeconds = mkOption {
        type = types.int;
        default = 5;
        description = ''
          Seconds the drainer sleeps between idle queue polls. Matches the
          importer worker's poll cadence; tune downward only if the operator
          wants tighter latency on rescue jobs.
        '';
      };
      sourceAddress = mkOption {
        type = types.str;
        default = "";
        example = "192.168.1.36";
        description = ''
          Local IP to bind yt-dlp's client socket to (passed through as
          ``yt-dlp --source-address``). Leave empty for default-route
          egress. Set this to the host's VPN-routed NIC IP so YouTube
          egress is policy-routed through the upstream VPN, the same way
          slskd's traffic is routed: the host's source-IP routing rule
          (``ip rule from <addr> lookup <table>``) sends sockets bound to
          this address out the VPN interface. The worker's DB/control
          traffic is unaffected because only yt-dlp binds to this address.
          This is host-specific, so it lives in the downstream wrapper, not
          the in-flake module's defaults (KTD9).
        '';
      };
    };

    slskd = {
      apiKeyFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = ''
          Path to a file containing the slskd API key (raw, no envvar prefix).
          Must be readable by services.cratedigger.user. Use sops/agenix or any
          out-of-band mechanism — the module just reads the file at runtime.

          Since issue #117 this path is written directly into the immutable
          application config and read on demand by the Python pipeline. No
          plaintext copy lives in the Nix store. If non-root
          tooling (e.g. pipeline-cli force-import) also needs to reach slskd,
          that operator user must be able to read this file too — typically
          done by mode 0440 + an operator group, not by loosening config.ini.
        '';
      };
      hostUrl = mkOption {
        type = types.str;
        default = "http://localhost:5030";
        description = "slskd HTTP base URL.";
      };
      urlBase = mkOption {
        type = types.str;
        default = "/";
        description = "slskd URL prefix when behind a reverse proxy.";
      };
      downloadDir = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = ''
          Directory slskd downloads land in.  When set, must be an
          absolute normalized path (no trailing slash, no . or ..
          components, no doubled slashes).
        '';
      };
      deleteSearches = mkOption {
        type = types.bool;
        default = true;
      };
      stalledTimeout = mkOption {
        type = types.int;
        default = 600;
        description = "Seconds before a stalled download is abandoned.";
      };
      remoteQueueTimeout = mkOption {
        type = types.int;
        default = 3600;
        description = "Seconds before a remote-queued download is abandoned.";
      };
    };

    pipelineDb = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Use the pipeline DB as album source (currently the only supported mode).";
      };
      createLocally = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Provision PostgreSQL on this host (services.postgresql +
          ensureDatabases/ensureUsers). The ensure-role and database are
          named after services.cratedigger.user, so unix-socket PEER
          authentication works by construction — no password material
          anywhere (KTD5). The DSN defaults to the local socket and
          cratedigger-db-migrate is ordered after postgresql-setup.service so
          first boot cannot race role and database provisioning. The operator's external-DB
          setup (createLocally = false + an explicit dsn) is unchanged.
        '';
      };
      dsn = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "postgresql://cratedigger@localhost/cratedigger";
        description = ''
          PostgreSQL connection string for the pipeline DB. Required
          unless pipelineDb.createLocally = true (which defaults it to
          the local unix socket).
        '';
      };
    };

    redis = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the local Redis server owned by cratedigger for peer-cache and web metadata caching.";
      };
      host = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Redis bind/client host used by the app-owned cratedigger Redis server.";
      };
      port = mkOption {
        type = types.port;
        default = 6379;
      };
      maxmemory = mkOption {
        type = types.str;
        default = "3gb";
        description = "Redis maxmemory setting for the app-owned cratedigger server.";
      };
    };

    peerCache = {
      ttlSeconds = mkOption {
        type = types.int;
        default = 7 * 24 * 60 * 60;
        description = "TTL in seconds for Redis peer_dir, peer_dir_neg, and peer_dir_count entries.";
      };
      speedTtlSeconds = mkOption {
        type = types.int;
        default = 24 * 60 * 60;
        description = "TTL in seconds for Redis peer_speed entries.";
      };
      redisConnectTimeoutMs = mkOption {
        type = types.int;
        default = 200;
        description = "Redis connect timeout for the pipeline peer cache, in milliseconds.";
      };
      redisOperationTimeoutMs = mkOption {
        type = types.int;
        default = 100;
        description = "Redis command timeout for the pipeline peer cache, in milliseconds.";
      };
    };

    # Cratedigger consumes one external Beets runtime capability. Package,
    # effective configuration, secret delivery, state provisioning, database,
    # library root, and the operator CLI remain deployment-owned.
    beets = {
      runtime = {
        package = mkOption {
          type = types.nullOr types.package;
          default = null;
          description = ''
            External Beets Python package consumed by every Cratedigger
            application and checker. Required when Cratedigger is enabled and
            must use services.cratedigger.packageSet.python3.
          '';
        };
        configDir = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "External immutable BEETSDIR. Required when Cratedigger is enabled.";
        };
        expectedLibrary = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Canonical external Beets SQLite database path. Required when Cratedigger is enabled.";
        };
        expectedDirectory = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Canonical external Beets library root. Required when Cratedigger is enabled.";
        };
        expectedStateFile = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Externally provisioned host-local Beets state file. Required when Cratedigger is enabled.";
        };
        expectedSecretInclude = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Designated external token-only Beets secret include. Required when Cratedigger is enabled.";
        };
        readinessUnits = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Deployment-owned units that must complete before guarded Cratedigger applications start.";
        };
      };

      validation = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Validate every download against MusicBrainz via beets before import.";
        };
        harnessPath = mkOption {
          type = types.str;
          default = "${cfg.src}/harness/run_beets_harness.sh";
          defaultText = lib.literalExpression "\${cfg.src}/harness/run_beets_harness.sh";
          description = "Path to the beets harness wrapper script.";
        };
        distanceThreshold = mkOption {
          type = types.float;
          default = 0.15;
          description = "Maximum beets match distance to accept (0.0 = perfect, 1.0 = no match).";
        };
        stagingDir = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Directory to stage validated albums for beets import. Required when beets.validation.enable.";
        };
        trackingFile = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "JSONL file tracking beets validation results. Required when beets.validation.enable.";
        };
        verifiedLosslessTarget = mkOption {
          type = types.str;
          default = "";
          description = "Target format after verified lossless (e.g. 'opus 128', 'mp3 v2'). Empty = keep V0.";
        };
      };
    };

    musicbrainz = {
      apiBase = mkOption {
        type = types.str;
        default = "https://musicbrainz.org";
        example = "http://mb-mirror.lan:5200";
        description = ''
          MusicBrainz API origin (scheme://host[:port], no path) — ONE value
          threaded to all consumers (tier-2 plan U6/KTD6): web/mb.py
          (via immutable runtime config [MusicBrainz] api_base, read at cratedigger-web
          startup by configure_api_bases_from_runtime_config()), pipeline-cli
          release lookups, and DatabaseSource track population. The external
          Beets owner configures its own MusicBrainz endpoint. Public MB is
          functional but rate-limited (~1 req/s); point at a local mirror for
          production-speed matching.
        '';
      };
    };

    discogs = {
      apiBase = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "https://discogs.ablz.au";
        description = ''
          Discogs mirror origin for web browse and DatabaseSource track
          population. Mirror-REQUIRED (R13): the Rust mirror's endpoint shape
          is not served by public api.discogs.com, so there is no public
          fallback. Null = Discogs browse off (clear 503 mirror-required
          message); MusicBrainz browse is unaffected. The external Beets owner
          separately configures the Discogs plugin.
        '';
      };
    };

    web = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Run the web UI behind the module-owned loopback authentication
          gateway and permissioned Unix backend socket.
        '';
      };
      hostName = mkOption {
        type = types.nullOr types.nonEmptyStr;
        default = null;
        example = "music.example.net";
        description = ''
          Lowercase canonical public DNS hostname for browser Host and
          same-origin validation. IP literals are rejected. Required when
          the web UI is enabled.
        '';
      };
      gatewayPort = mkOption {
        type = types.port;
        default = 8086;
        description = ''
          Port for the module-owned nginx authentication gateway. Listener
          addresses are controlled by gatewayAddresses; the public TLS reverse
          proxy should forward to this port.
        '';
      };
      gatewayAddresses = mkOption {
        type = types.nonEmptyListOf types.nonEmptyStr;
        default = ["127.0.0.1"] ++ optional config.networking.enableIPv6 "[::1]";
        defaultText = lib.literalExpression ''["127.0.0.1"] ++ lib.optional config.networking.enableIPv6 "[::1]"'';
        description = ''
          Addresses for the module-owned nginx authentication gateway. The
          default is loopback-only. A deployment may add an address belonging
          to a private container bridge when a separately isolated ingress
          sidecar must reach the gateway; perimeter firewall policy remains
          deployment-owned.
        '';
      };
      accessGroup = mkOption {
        type = types.nonEmptyStr;
        default = "cratedigger-web";
        description = ''
          Dedicated group authorized to connect to the web backend Unix
          socket. Add only trusted local pipeline-cli operators explicitly.
          It must not reuse root, wheel, the Cratedigger service/media group,
          nginx's primary group, cratedigger-ops, users, or the configured
          Discogs operator group. The module cannot infer the purpose of
          arbitrary other groups, which remain the consumer's responsibility.
        '';
      };
      basicAuthFile = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "/run/secrets/cratedigger.htpasswd";
        description = ''
          Runtime htpasswd file used by the module-owned nginx gateway.
          The configured and resolved paths must remain outside the Nix store
          under root-owned, non-writable ancestors. The target must be
          root-owned, non-empty, mode root:nginx 0440, readable by nginx, and
          unreadable by the application and non-nginx socket users.
        '';
      };
      enableInsecure = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Deliberately run the web gateway without browser authentication.
          This is mutually exclusive with basicAuthFile and externalAuth; the
          Unix socket, request provenance checks, and other security
          boundaries remain.
        '';
      };
      externalAuth = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Declare that an established external component in front of this
          gateway owns browser authorization, as a whole-site allow or deny
          decision. Use this when a reverse proxy you operate authenticates
          every request before forwarding it to gatewayPort — for example an
          OIDC provider fronted by forward authentication.

          Cratedigger performs no authorization itself in this mode. It sends
          no sub-request to the authorizer and does not probe whether one is
          reachable, so selecting this mode is an assertion you make about
          your own deployment, not one the module verifies. If the component
          in front fails open, the gateway is served anonymously and
          Cratedigger cannot detect it.

          The deployment contract this mode depends on: your proxy runs on the
          same host, reaches the gateway on its loopback port, and forwards the
          canonical hostName as the Host header. Provider identity, roles,
          cookies, and tokens are dropped by the gateway's reviewed header set
          and never reach the application.

          This is mutually exclusive with basicAuthFile and enableInsecure;
          there is no fallback between modes. The Unix socket, request
          provenance checks, security headers, and the anonymous /healthz
          exception are unchanged.
        '';
      };
      redis = {
        host = mkOption {
          type = types.str;
          default = "127.0.0.1";
          description = "Redis host for web metadata caching. Defaults to the app-owned cratedigger Redis host.";
        };
        port = mkOption {
          type = types.port;
          default = 6379;
        };
      };
    };

    # Notifier credential *File options follow the same contract as
    # slskd.apiKeyFile (issue #117): paths written into immutable config, read on
    # demand by CratediggerConfig.resolved_*(). They must be readable by
    # services.cratedigger.user. If the operator also triggers imports via
    # pipeline-cli from a non-root shell, the same files must be readable
    # by that user too, otherwise notifier scans silently no-op after
    # CLI-triggered imports (the import itself still succeeds).
    notifiers = {
      plex = {
        enable = mkEnableOption "Plex post-import scanner notifier";
        url = mkOption {
          type = types.str;
          default = "";
          example = "https://plex.example.com";
        };
        tokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
        librarySectionId = mkOption {
          type = types.int;
          default = 0;
          description = "Plex library section ID (numeric).";
        };
        pathMap = mkOption {
          type = types.str;
          default = "";
          example = "/mnt/virtio/Music/Beets:/prom_music";
          description = "host:container path remap for partial-section refreshes.";
        };
      };
      jellyfin = {
        enable = mkEnableOption "Jellyfin post-import scanner notifier";
        url = mkOption {
          type = types.str;
          default = "";
        };
        tokenFile = mkOption {
          type = types.nullOr types.path;
          default = null;
        };
        pathMap = mkOption {
          type = types.str;
          default = "";
          example = "/mnt/virtio/Music/Beets:/mnt/fuse/Media/Music/Beets";
          description = ''
            local:remote path remap from the beets library path on this host
            to the path Jellyfin sees. Used by both the album-path media
            update and the "Recently Added" DateCreated pin.
          '';
        };
      };
    };

    healthCheck = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Verify slskd is connected before running each cycle.";
      };
      onFailureCommand = mkOption {
        type = types.str;
        default = "";
        example = "systemctl restart slskd.service";
        description = ''
          Shell command to run when the health check fails. Empty = log and skip
          the run. The command is invoked as root (the health-check ExecStartPre is
          "+"-prefixed, so it runs as root even when services.cratedigger.user is
          non-root — e.g. so `systemctl restart slskd.service` works) and
          retries are attempted for up to a minute after it returns.
        '';
      };
    };

    releaseSettings = {
      useMostCommonTracknum = mkOption { type = types.bool; default = true; };
      allowMultiDisc = mkOption { type = types.bool; default = true; };
      acceptedCountries = mkOption {
        type = types.listOf types.str;
        default = ["Europe" "Japan" "United Kingdom" "United States" "[Worldwide]" "Australia" "Canada"];
      };
      skipRegionCheck = mkOption { type = types.bool; default = false; };
      acceptedFormats = mkOption {
        type = types.listOf types.str;
        default = ["CD" "Digital Media" "Vinyl"];
      };
    };

    searchSettings = {
      searchTimeout = mkOption { type = types.int; default = 30000; description = "Milliseconds."; };
      maximumPeerQueue = mkOption { type = types.int; default = 50; };
      minimumPeerUploadSpeed = mkOption { type = types.int; default = 0; };
      minimumFilenameMatchRatio = mkOption { type = types.float; default = 0.6; };
      allowedFiletypes = mkOption {
        type = types.listOf types.str;
        default = ["flac 24/192" "flac 24/96" "flac 24/48" "flac 16/44.1" "flac" "alac" "mp3 v0" "mp3 320" "aac" "opus" "ogg" "mp3" "wav"];
        description = ''
          Priority-ordered filetype filter. The rank model in lib/quality/ranks.py is
          the authoritative quality decision (post-download); this filter is
          only for search-time peer/codec preference.
        '';
      };
      ignoredUsers = mkOption {
        type = types.listOf types.str;
        default = [];
      };
      searchForTracks = mkOption { type = types.bool; default = true; };
      albumPrependArtist = mkOption { type = types.bool; default = true; };
      trackPrependArtist = mkOption { type = types.bool; default = true; };
      searchType = mkOption {
        type = types.enum ["incrementing_page" "all_at_once"];
        default = "incrementing_page";
      };
      parallelSearches = mkOption { type = types.int; default = 8; };
      numberOfAlbumsToGrab = mkOption {
        type = types.int;
        default = 16;
        description = ''
          Eligible requests selected per pipeline cycle. Must be at least 2 so
          new-request priority cannot starve the established library.
        '';
      };
      searchResponseLimit = mkOption {
        type = types.int;
        default = 1000;
        description = ''
          Caps how many peer responses slskd collects per search. Maps to
          slskd's `responseLimit` ceiling — raising this lets the matcher
          consider more peers per query at the cost of a longer search window.
        '';
      };
      searchFileLimit = mkOption {
        type = types.int;
        default = 50000;
        description = ''
          Caps how many total files slskd collects across all peer responses
          per search. Maps to slskd's `fileLimit` ceiling. The slskd-api
          default (10000) terminates popular multi-disc searches in a few
          seconds — possibly before the right peer responds. 50000 gives the
          matcher more peer diversity for albums where each peer holds 50+
          files (compilations, OSTs, multi-disc reissues).
        '';
      };
      browseTopK = mkOption {
        type = types.int;
        default = 20;
        description = ''
          First wave size for parallel peer browse fan-out. After ranking
          eligible peers by upload speed, the top K are browsed concurrently
          and the cache is matched against them. If no match is found, the
          tail is browsed in further chunks of K. Tune downward if first-match
          rank is consistently low; tune upward only if browse budget allows.
          See issue #198.
        '';
      };
      browseGlobalMaxWorkers = mkOption {
        type = types.int;
        default = 32;
        description = ''
          Global cap on the ThreadPoolExecutor used by browse fan-out. Limits
          simultaneous in-flight `users.directory()` calls across all users
          and all dirs in a wave. Higher than browseTopK so a single user
          contributing many candidate dirs still gets meaningful parallelism.
          Watch slskd's own logs for serialisation if raised.
        '';
      };
      searchMaxInflight = mkOption {
        type = types.int;
        default = 4;
        description = ''
          Pipeline depth for the parallel search executor — number of
          in-flight search-collection futures at once. Submission stays
          sequential (slskd's `SearchRequestLimiter` is on POST only, with
          a built-in 429-retry loop), but the collect-side workload runs
          in this many threads. Raised from the legacy hard-coded 2 once
          browse fan-out (issue #198) stops being the dominant cost.
        '';
      };
      titleBlacklist = mkOption {
        type = types.listOf types.str;
        default = [];
      };
      searchBlacklist = mkOption {
        type = types.listOf types.str;
        default = [];
      };
    };

    downloadSettings = {
      downloadFiltering = mkOption { type = types.bool; default = true; };
      useExtensionWhitelist = mkOption { type = types.bool; default = false; };
      extensionsWhitelist = mkOption {
        type = types.listOf types.str;
        default = ["lrc" "nfo" "txt"];
      };
    };

    qualityRanks = {
      bitrateMetric = mkOption {
        type = types.enum ["min" "avg" "median"];
        default = "avg";
      };
      withinRankToleranceKbps = mkOption {
        type = types.int;
        default = 5;
      };
      bands = {
        opus = mkCodecBands "Opus" {
          transparent = 112;
          excellent = 88;
          good = 64;
          acceptable = 48;
        };
        mp3 = mkCodecBands "MP3" {
          transparent = 320;
          excellent = 256;
          good = 192;
          acceptable = 128;
        };
        aac = mkCodecBands "AAC" {
          transparent = 192;
          excellent = 144;
          good = 112;
          acceptable = 80;
        };
        vorbis = mkCodecBands "Vorbis" {
          transparent = 192;
          excellent = 160;
          good = 112;
          acceptable = 96;
        };
        wma = mkCodecBands "WMA" {
          transparent = 320;
          excellent = 256;
          good = 192;
          acceptable = 128;
        };
      };
    };

    logging = {
      level = mkOption { type = types.str; default = "INFO"; };
      format = mkOption {
        type = types.str;
        default = "[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s";
      };
      datefmt = mkOption {
        type = types.str;
        default = "%Y-%m-%dT%H:%M:%S%z";
      };
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = canonicalStateDirIsValid;
        message = "services.cratedigger.stateDir must be an absolute normalized non-root path without a trailing slash (for example, /var/lib/cratedigger).";
      }
      {
        assertion = applicationUserIsNonRoot && applicationGroupIsNonRoot;
        message = "services.cratedigger guarded application identity must use a named non-root user and group (numeric systemd identity spellings are forbidden, and neither name nor resolved UID/GID may be 0).";
      }
      {
        assertion = cfg.beets.runtime.package != null;
        message = "services.cratedigger.beets.runtime.package is required: supply the external Beets Python package used by this deployment.";
      }
      {
        assertion = cfg.beets.runtime.configDir != null;
        message = "services.cratedigger.beets.runtime.configDir is required: supply the external immutable BEETSDIR.";
      }
      {
        assertion = cfg.beets.runtime.expectedLibrary != null;
        message = "services.cratedigger.beets.runtime.expectedLibrary is required: supply the canonical Beets SQLite database path.";
      }
      {
        assertion = cfg.beets.runtime.expectedDirectory != null;
        message = "services.cratedigger.beets.runtime.expectedDirectory is required: supply the canonical Beets library root.";
      }
      {
        assertion = cfg.beets.runtime.expectedStateFile != null;
        message = "services.cratedigger.beets.runtime.expectedStateFile is required: supply the externally provisioned host-local Beets state file.";
      }
      {
        assertion = cfg.beets.runtime.expectedSecretInclude != null;
        message = "services.cratedigger.beets.runtime.expectedSecretInclude is required: supply the designated token-only Beets secret include.";
      }
      {
        assertion =
          cfg.beets.runtime.package == null
          || (
            cfg.beets.runtime.package ? pythonModule
            && cfg.beets.runtime.package.pythonModule == cfg.packageSet.python3
          );
        message = "services.cratedigger.beets.runtime.package.pythonModule must match services.cratedigger.packageSet.python3 so every application and checker uses one Beets runtime.";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.beets.runtime.configDir;
        message = "services.cratedigger.beets.runtime.configDir must be an absolute normalized path.";
      }
      {
        assertion = cfg.beets.runtime.configDir != "/";
        message = "services.cratedigger.beets.runtime.configDir must not be /; a root BEETSDIR disables sandbox filesystem isolation.";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.beets.runtime.expectedLibrary;
        message = "services.cratedigger.beets.runtime.expectedLibrary must be an absolute normalized path.";
      }
      {
        assertion = cfg.beets.runtime.expectedLibrary == null || dirOf cfg.beets.runtime.expectedLibrary != "/";
        message = "services.cratedigger.beets.runtime.expectedLibrary parent directory must not be /; its mutation capability must remain narrower than the host root.";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.beets.runtime.expectedDirectory;
        message = "services.cratedigger.beets.runtime.expectedDirectory must be an absolute normalized path.";
      }
      {
        assertion = cfg.beets.runtime.expectedDirectory != "/";
        message = "services.cratedigger.beets.runtime.expectedDirectory must not be /; a root library capability disables sandbox filesystem isolation.";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.beets.runtime.expectedStateFile;
        message = "services.cratedigger.beets.runtime.expectedStateFile must be an absolute normalized path.";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.beets.runtime.expectedSecretInclude;
        message = "services.cratedigger.beets.runtime.expectedSecretInclude must be an absolute normalized path.";
      }
      {
        assertion = cfg.slskd.apiKeyFile != null;
        message = "services.cratedigger.slskd.apiKeyFile is not set: point it at a file containing your slskd API key (readable by services.cratedigger.user).";
      }
      {
        assertion = cfg.slskd.downloadDir != null;
        message = "services.cratedigger.slskd.downloadDir is not set: point it at the directory slskd downloads land in (slskd's directories.downloads).";
      }
      {
        assertion = isAbsoluteNormalizedPath cfg.processingDir;
        message = "services.cratedigger.processingDir must be an absolute normalized path (no trailing slash, no . or .. components, no doubled slashes).";
      }
      {
        assertion = cfg.slskd.downloadDir == null || isAbsoluteNormalizedPath cfg.slskd.downloadDir;
        message = "services.cratedigger.slskd.downloadDir must be an absolute normalized path when set (no trailing slash, no . or .. components, no doubled slashes).";
      }
      {
        assertion = cfg.slskd.downloadDir == null || (
          let
            processing = lib.removeSuffix "/" cfg.processingDir;
            downloads = lib.removeSuffix "/" cfg.slskd.downloadDir;
          in !(lib.hasPrefix "${processing}/" downloads || lib.hasPrefix "${downloads}/" processing || processing == downloads)
        );
        message = "services.cratedigger.processingDir must be lexically disjoint from services.cratedigger.slskd.downloadDir";
      }
      {
        assertion = !cfg.localImport.enable || (
          cfg.localImport.dir != null
          && isAbsoluteNormalizedPath cfg.localImport.dir
          && cfg.localImport.dir != "/"
        );
        message = "services.cratedigger.localImport: enable requires localImport.dir to be set, an absolute normalized path (no trailing slash, no . or .. components), and not /.";
      }
      {
        assertion = cfg.pipelineDb.createLocally || cfg.pipelineDb.dsn != null;
        message = "services.cratedigger.pipelineDb: set either pipelineDb.dsn (external PostgreSQL) or pipelineDb.createLocally = true (provision a local database with peer auth).";
      }
      {
        assertion = !cfg.beets.validation.enable || (cfg.beets.validation.stagingDir != null && cfg.beets.validation.trackingFile != null);
        message = "services.cratedigger.beets.validation: enable requires stagingDir (where validated albums stage for import) and trackingFile (JSONL validation log).";
      }
      {
        # The rescue worker stages into the same beets Incoming root; an
        # unset stagingDir would silently render --staging-dir "" and
        # strand rescues under the state dir.
        assertion = !cfg.youtubeIngest.enable || cfg.beets.validation.stagingDir != null;
        message = "services.cratedigger.youtubeIngest: enable requires beets.validation.stagingDir (rescues stage under its auto-import/ child).";
      }
      {
        assertion = lib.hasPrefix "http://" cfg.musicbrainz.apiBase || lib.hasPrefix "https://" cfg.musicbrainz.apiBase;
        message = "services.cratedigger.musicbrainz.apiBase must be an origin URL (scheme://host[:port], no path), e.g. https://musicbrainz.org or http://mb-mirror.lan:5200.";
      }
      {
        assertion = cfg.discogs.apiBase == null || lib.hasPrefix "http://" cfg.discogs.apiBase || lib.hasPrefix "https://" cfg.discogs.apiBase;
        message = "services.cratedigger.discogs.apiBase must be an origin URL (scheme://host[:port]) when set, e.g. https://discogs.ablz.au.";
      }
      {
        assertion = !cfg.notifiers.plex.enable || (cfg.notifiers.plex.tokenFile != null && cfg.notifiers.plex.url != "");
        message = "services.cratedigger.notifiers.plex: enable requires url and tokenFile";
      }
      {
        assertion = !cfg.notifiers.jellyfin.enable || (cfg.notifiers.jellyfin.tokenFile != null && cfg.notifiers.jellyfin.url != "");
        message = "services.cratedigger.notifiers.jellyfin: enable requires url and tokenFile";
      }
      {
        assertion = cfg.importer.previewWorkers >= 1;
        message = "services.cratedigger.importer.previewWorkers must be at least 1";
      }
      {
        assertion = cfg.searchSettings.numberOfAlbumsToGrab >= 2;
        message = "services.cratedigger.searchSettings.numberOfAlbumsToGrab must be at least 2";
      }
      {
        assertion = !cfg.web.enable || webModeCount == 1;
        message = "services.cratedigger.web requires exactly one authentication mode: set basicAuthFile, or explicitly set enableInsecure = true, or set externalAuth = true.";
      }
      {
        assertion = !cfg.web.enable || !(webBasicEnabled && cfg.web.enableInsecure);
        message = "services.cratedigger.web basicAuthFile and enableInsecure are mutually exclusive.";
      }
      {
        assertion = !cfg.web.enable || !(webBasicEnabled && webExternalEnabled);
        message = "services.cratedigger.web basicAuthFile and externalAuth are mutually exclusive; external authorization never falls back to Basic.";
      }
      {
        assertion = !cfg.web.enable || !(cfg.web.enableInsecure && webExternalEnabled);
        message = "services.cratedigger.web enableInsecure and externalAuth are mutually exclusive.";
      }
      {
        assertion = !cfg.web.enable || config.services.nginx.enableReload;
        message = "services.cratedigger.web requires services.nginx.enableReload = true so authentication-policy changes fail closed without stopping unrelated nginx virtual hosts.";
      }
      {
        assertion = !cfg.web.enable || config.systemd.services.nginx.restartIfChanged;
        message = "services.cratedigger.web requires systemd.services.nginx.restartIfChanged = true so the first authenticated enable and service-identity changes restart nginx to acquire the module-owned socket group.";
      }
      {
        assertion = !cfg.web.enable || webHostNameIsValid;
        message = "services.cratedigger.web.hostName must be a lowercase canonical DNS hostname, not an IP literal.";
      }
      {
        assertion = !cfg.web.enable || cfg.web.gatewayPort != 8085;
        message = "services.cratedigger.web.gatewayPort must not reuse the retired Python TCP port 8085.";
      }
      {
        assertion = !cfg.web.enable || builtins.match "[a-z_][a-z0-9_-]*" cfg.web.accessGroup != null;
        message = "services.cratedigger.web.accessGroup must be a valid dedicated Linux group name.";
      }
      {
        assertion = !cfg.web.enable || webAccessGroupIsSafe;
        message = "services.cratedigger.web.accessGroup must be dedicated: it must differ from nginx's primary group and must not reuse a forbidden authority group (root, wheel, the Cratedigger service/media group, cratedigger-ops, or users).";
      }
      {
        assertion = !cfg.web.enable || cfg.user != config.services.nginx.user;
        message = "services.cratedigger.web requires distinct application and nginx service users.";
      }
      {
        assertion =
          !cfg.web.enable
          || webNginxServiceIdentityIsModuleOwned;
        message = "services.cratedigger.web requires the final nginx.service User and Group to remain services.nginx.user and services.nginx.group.";
      }
      {
        assertion = !cfg.web.enable || webNginxUserIsSafe;
        message = "services.cratedigger.web requires the nginx worker user to resolve away from UID 0.";
      }
      {
        assertion = !cfg.web.enable || webNginxPrimaryGroupIsSafe;
        message = "services.cratedigger.web requires nginx's primary group and resolved GID to differ from Cratedigger secret/media authority groups.";
      }
      {
        assertion = !cfg.web.enable || webNginxForbiddenGroupOverlap == [];
        message = "services.cratedigger.web forbids nginx account/service membership in root, wheel, cfg.group, cratedigger-ops, or users.";
      }
      {
        assertion = !cfg.web.enable || webNginxHasAccessGroup;
        message = "services.cratedigger.web requires nginx account/service membership in web.accessGroup so the gateway can reach its Unix socket.";
      }
      {
        assertion = !cfg.web.enable || !webBasicEnabled || webBasicAuthPathIsValid;
        message = "services.cratedigger.web.basicAuthFile must be a normalized absolute runtime path with nginx-token-safe segments outside /nix/store.";
      }
      {
        assertion =
          cfg.web.enable
          || (!webBasicEnabled && !cfg.web.enableInsecure && !webExternalEnabled);
        message = "services.cratedigger.web authentication settings are inactive-mode residue while web.enable is false.";
      }
      {
        assertion = !cfg.web.enable || !webBasicEnabled || cfg.user != "root";
        message = "services.cratedigger.web Basic authentication requires a non-root application user so the application cannot read the root:nginx credential file.";
      }
      {
        assertion = !cfg.web.enable || !webBasicEnabled || cfg.group != config.services.nginx.group;
        message = "services.cratedigger.web Basic authentication requires the application group to differ from the nginx credential-file group.";
      }
      {
        assertion =
          !cfg.web.enable
          || !webBasicEnabled
          || webApplicationCredentialGroupIsSafe;
        message = "services.cratedigger.web Basic authentication forbids the nginx credential-file group in cratedigger-web.service SupplementaryGroups.";
      }
      {
        assertion =
          !cfg.web.enable
          || !webBasicEnabled
          || webApplicationServiceIdentityIsModuleOwned;
        message = "services.cratedigger.web Basic authentication requires the final cratedigger-web.service User and Group to remain module-owned (services.cratedigger.user and services.cratedigger.group).";
      }
    ];

    environment.systemPackages = [pipelineCli pipelineMigrate decisionDifferential importerPkg previewWorkerPkg youtubeIngestWorkerPkg checkBeetsConfigPkg pkgs.postgresql];
    environment.etc."cratedigger/web-gateway-policy" = mkIf cfg.web.enable {
      text = webGatewayPolicyText;
      mode = "0444";
    };

    users.users = lib.mkMerge [
      (mkIf (cfg.user != "root") {
        ${cfg.user} = {
          isSystemUser = true;
          group = cfg.group;
          extraGroups = optional cfg.web.enable cfg.web.accessGroup;
          description = "Cratedigger service user";
        };
      })
      (mkIf cfg.web.enable {
        ${config.services.nginx.user}.extraGroups = [cfg.web.accessGroup];
      })
    ];
    users.groups = lib.mkMerge [
      (mkIf (cfg.group != "root") { ${cfg.group} = {}; })
      (mkIf cfg.web.enable { ${cfg.web.accessGroup} = {}; })
    ];

    # The state directory contains no config or plaintext secrets, so it can be
    # world-readable; services read the immutable store config. The
    # secrets themselves live at operator-chosen paths (see slskd.apiKeyFile
    # / notifiers.*.tokenFile) and retain their own
    # restrictive modes from whatever provisioned them (sops-nix, agenix, etc).
    systemd.tmpfiles.rules =
      [
        "d ${cfg.stateDir} 0755 ${cfg.user} ${cfg.group} -"
        "d ${cfg.processingDir} 0700 ${cfg.user} ${cfg.group} -"
        "d ${cfg.processingDir}/albums 0700 ${cfg.user} ${cfg.group} -"
        "d ${cfg.processingDir}/albums/failed_imports 0700 ${cfg.user} ${cfg.group} -"
        "d ${cfg.processingDir}/preview 0700 ${cfg.user} ${cfg.group} -"
        # Only ephemeral preview children are age-cleaned.  Canonical albums
        # are durable in-flight state and are never a tmpfiles cleanup target.
        "e ${cfg.processingDir}/preview 0700 ${cfg.user} ${cfg.group} 7d"
      ]
      # Parent traversal and socket access are separate boundaries: tmpfiles
      # owns root:<accessGroup> 0750 here, while the socket unit owns the
      # root:<accessGroup> 0660 node below it.
      ++ optional cfg.web.enable
        "d /run/cratedigger-web 0750 root ${cfg.web.accessGroup} -"
      ++ optional cfg.youtubeIngest.enable
        "d ${cfg.youtubeIngest.tempDir} 0755 ${cfg.user} ${cfg.group} -";

    # Local PostgreSQL (stranger ergonomics, KTD5): role + database named
    # after cfg.user so unix-socket peer auth works with zero credentials
    # (ensureDBOwnership requires database name == role name). The DSN
    # defaults to the socket; nothing for the *File secret pattern to
    # carry, no pg_hba loosening.
    services.postgresql = mkIf cfg.pipelineDb.createLocally {
      enable = true;
      ensureDatabases = [ cfg.user ];
      ensureUsers = [
        {
          name = cfg.user;
          ensureDBOwnership = true;
        }
      ];
    };
    services.cratedigger.pipelineDb.dsn = mkIf cfg.pipelineDb.createLocally (
      lib.mkDefault "postgresql:///${cfg.user}?host=/run/postgresql"
    );

    services.cratedigger.web.redis.host = lib.mkDefault cfg.redis.host;
    services.cratedigger.web.redis.port = lib.mkDefault cfg.redis.port;
    services.redis.servers.cratedigger = {
      enable = cfg.redis.enable;
      bind = cfg.redis.host;
      port = cfg.redis.port;
      settings = {
        maxmemory = cfg.redis.maxmemory;
        "maxmemory-policy" = "allkeys-lru";
      };
    };

    services.nginx.enable = mkIf cfg.web.enable true;
    services.nginx.enableReload = mkIf cfg.web.enable (lib.mkDefault true);
    services.nginx.virtualHosts = mkIf cfg.web.enable {
      cratedigger-auth-gateway = {
        serverName = webHostName;
        listen = webGatewayListen;
        # Own Basic at server scope so any downstream-added application
        # location inherits it. The sole anonymous exception disables the
        # inherited policy explicitly in the exact health location below.
        basicAuthFile =
          if webBasicEnabled
          then cfg.web.basicAuthFile
          else null;
        # Nginx normalizes an absolute-form request target before exposing
        # $request_uri. Reject it from the untouched request line so it cannot
        # acquire the exact origin-form /healthz exemption or reach the app.
        extraConfig = ''
          # Reload preparation removes this marker before validating a new
          # authentication policy. Keep the gate at server scope so health
          # and application routes both fail closed while policy is uncertain.
          if (!-f ${webGatewayActiveMarker}) {
            return 503;
          }
          if (''$request ~ "^[^ ]+ +[A-Za-z][A-Za-z0-9+.-]*://") {
            return 400;
          }
        '' + webProxyRequestConfig + webResourceIsolationConfig;
        locations."= /healthz" = {
          proxyPass = "http://unix:${webSocketPath}:/healthz";
          recommendedProxySettings = false;
          extraConfig = ''
            auth_basic off;
            if (''$request_uri != "/healthz") {
              return 404;
            }
            limit_except GET {
              deny all;
            }
          '' + webHealthProxyRequestConfig;
        };
        locations."/" = {
          proxyPass = "http://unix:${webSocketPath}:";
          recommendedProxySettings = false;
        };
      };
      cratedigger-auth-reject = {
        default = true;
        serverName = "_";
        listen = webGatewayListen;
        locations."/".extraConfig = "return 444;";
      };
    };

    # The worker needs only the new socket group. In particular it does not
    # receive cfg.group, cratedigger-ops, or any other Cratedigger secret/media
    # group. Basic-file metadata and gateway readiness are checked separately
    # around nginx start/reload.
    systemd.services.nginx = mkIf cfg.web.enable {
      after = ["cratedigger-web.socket"];
      # The socket unit is activation-managed and may be stopped/restarted by
      # switch-to-configuration even when its effective definition is
      # unchanged. A hard Requires= edge propagates that transient stop to the
      # shared nginx master, defeating reload-only authentication-policy
      # changes and taking unrelated virtual hosts down. Wants= still brings
      # the socket up with nginx; the application retains the hard Requires=
      # boundary and nginx returns an ordinary upstream failure during any
      # brief socket transition.
      wants = ["cratedigger-web.socket"];
      serviceConfig = lib.mkMerge [
        {
          SupplementaryGroups = [cfg.web.accessGroup];
          ExecStartPre = lib.mkBefore [
            "+${webGatewayStartClearScript}"
            webNginxEffectiveIdentityScript
            "+${webGatewayStartScript}"
          ];
        }
        {
          ExecReload = lib.mkBefore [
            "+${webGatewayReloadPrepareScript}"
          ];
        }
        {
          ExecReload = lib.mkAfter [
            "+${webGatewayReloadFinishScript}"
          ];
        }
      ];
    };

    systemd.sockets.cratedigger-web = mkIf cfg.web.enable {
      description = "Cratedigger web backend socket";
      wantedBy = ["sockets.target"];
      listenStreams = [webSocketPath];
      socketConfig = {
        SocketUser = "root";
        SocketGroup = cfg.web.accessGroup;
        SocketMode = "0660";
        DirectoryMode = "0750";
        RemoveOnStop = true;
      };
    };

    # Schema migrator. RemainAfterExit=true so cratedigger-web (and the other
    # long-running/Requires= units below) can require us without re-running
    # on every cycle. Idempotent — fast no-op when schema is already current.
    # The two timer-driven, restartIfChanged=false units (cratedigger,
    # cratedigger-unfindable) deliberately do NOT require us: this unit's
    # ExecStart store path changes on every code deploy, so it restarts on
    # every switch, and systemd Requires= propagates that restart as a
    # SIGTERM to anything requiring it — killing a mid-flight cycle even
    # though the app code didn't change. Those two use Wants=+After= instead
    # and gate on schema currency themselves at startup
    # (lib.migrator.assert_schema_current) so a failed/behind migration still
    # blocks them from running. Long-running units that already
    # restartIfChanged=true on deploy (importer, preview worker, web,
    # youtube-ingest) keep Requires= — the propagated restart is harmless for
    # them.
    systemd.services.cratedigger-db-migrate = {
      description = "Apply Cratedigger pipeline DB schema migrations";
      wantedBy = ["multi-user.target"];
      # With a locally provisioned DB, PostgreSQL accepting connections is
      # not enough: NixOS creates ensureDatabases/ensureUsers in its separate
      # setup oneshot. Migration must wait for that role/database authority.
      after = optional cfg.pipelineDb.createLocally "postgresql-setup.service";
      requires = optional cfg.pipelineDb.createLocally "postgresql-setup.service";
      restartIfChanged = true;
      # Issue #1161: a switch must re-run the migrator even when something
      # else starts this unit concurrently. With the NixOS default
      # (stopIfChanged = true) switch-to-configuration puts us in its stop
      # list AND its start list. Our stop job is ordered behind every
      # Requires= dependent's stop (reverse After=), so while a slow worker
      # drains, that stop job is still queued -- and any concurrent
      # `systemctl start` (mode "replace") REPLACES it. RemainAfterExit
      # leaves us active(exited) throughout, so the replacement start hits
      # unit_start()'s -EALREADY: ExecStart never forks and systemd logs
      # nothing at all. The migration is silently skipped while the unit
      # still reports active/exited/success from hours earlier.
      #
      # stopIfChanged = false moves us to switch-to-configuration's restart
      # list, which issues a JOB_RESTART. systemd's job-merge table collapses
      # JOB_START into JOB_RESTART (restart wins), so a concurrent start can
      # no longer swallow the re-run. The restart phase also runs after the
      # stop phase and before the start phase, so the migration completes
      # before the Requires= workers come back up.
      #
      # Effect on the Requires= dependents, precisely: on a full switch they
      # are NOT bounced by this restart -- they were already stopped in the
      # stop phase, so the propagated try-restart collapses to a no-op and
      # they come back in the start phase as before. On a switch where this
      # unit is the ONLY thing that changed, they ARE try-restarted, which is
      # what an operator running `systemctl restart cratedigger-db-migrate`
      # already sees today.
      #
      # One further delta: if the migration FAILS, the start phase's workers
      # pull a fresh start job for this unit through their Requires=, so a
      # failing migrator now runs twice per switch deterministically (it was
      # nondeterministic before). Harmless -- the migrator is version-tracked
      # and idempotent -- but worth knowing when reading the journal.
      stopIfChanged = false;
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.user;
        Group = cfg.group;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${pipelineMigrate}/bin/pipeline-migrate";
      };
    };

    systemd.services.cratedigger = {
      description = "Cratedigger — Soulseek download pipeline";
      after = ["cratedigger-db-migrate.service"] ++ redisServiceUnits ++ beetsReadinessUnits;
      wants = ["cratedigger-db-migrate.service"] ++ redisServiceUnits ++ beetsReadinessUnits;
      restartIfChanged = false;
      # Deliberately exclude pythonEnv from PATH: the python interpreter is
      # invoked via absolute path inside the wrappers, and every Beets
      # consumer resolves the supplied interpreter from the guarded
      # [Beets] runtime keys (config_dir / python) rather than
      # PATH lookup — keeping PATH lean avoids ever re-introducing an
      # ambient-beet dependency (tier-2 plan R6).
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        # slskdHealthCheck is prefixed with "+" so it always runs as root,
        # regardless of cfg.user: its onFailureCommand (e.g. `systemctl
        # restart slskd.service`) needs root, and once cfg.user is
        # non-root a bare ExecStartPre would run as that user and be
        # unable to restart slskd. The second prestart only clears the main
        # pipeline's singleton lock; the application entrypoint owns Beets
        # validation and immutable configuration is never rendered here.
        ExecStartPre = lib.optional cfg.healthCheck.enable "+${slskdHealthCheck}" ++ [pipelinePreStartScript];
        BindReadOnlyPaths = beetsMainReadOnlyPaths;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${cratediggerPkg}/bin/cratedigger";
        WorkingDirectory = cfg.stateDir;
        # Defense-in-depth (issue #212 R13): if anything escapes the
        # in-band 90s per-search progress watchdog (clock-injection bug,
        # TCP socket hang inside the watchdog itself, etc.), systemd
        # SIGTERMs the process at 60 min. Healthy cycles run well under
        # 60 min so this never fires; the systemd timer simply schedules
        # the next cycle. Cycle-boundary checkpointing already tolerates
        # a forced kill — the importer service owns beets writes
        # independently and is unaffected.
        #
        # `RuntimeMaxSec` does NOT apply to Type=oneshot — systemd warns
        # `RuntimeMaxSec= has no effect in combination with Type=oneshot.
        # Ignoring.` and the defense-in-depth silently disappears. For a
        # oneshot, the entire service runtime IS the start phase, so
        # `TimeoutStartSec` is the right knob. It SIGTERMs (then SIGKILLs
        # after `TimeoutStopSec`) the ExecStart process at the cap.
        TimeoutStartSec = "1h";
      };
    };

    systemd.timers.cratedigger = mkIf cfg.timer.enable {
      description = "Cratedigger periodic run timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnBootSec = cfg.timer.onBootSec;
        OnUnitInactiveSec = cfg.timer.onUnitInactiveSec;
        Persistent = true;
      };
    };

    # Unfindable detection oneshot + daily timer. Lives in its own
    # systemd unit, NOT inline in the main cratedigger.service loop,
    # because R20 ("the system never stops searching") forbids the
    # regular search cadence from being throttled by detection state.
    # The structural separation makes that invariant enforceable: this
    # process shares no code path with the regular plan loop and
    # cannot accidentally mutate plan cursors.
    systemd.services.cratedigger-unfindable = {
      description = "Cratedigger unfindable detection oneshot";
      after = ["cratedigger-db-migrate.service" "network.target"];
      wants = ["cratedigger-db-migrate.service"];
      restartIfChanged = false;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        # Same health-check shape as cratedigger.service (including the "+"
        # root-escalation prefix — see the comment there). The detection job
        # never owns the main pipeline
        # lock, so it must not clear that lock while a cycle is active. It
        # gates on slskd reachability when the operator has health-check enabled
        # and hits slskd just as much as the main loop does, so a slskd
        # outage that's already down BEFORE the run starts fails the unit
        # fast here rather than launching a doomed run. An outage that
        # starts MID-RUN (issue #1090: a burst of transient 409s from a
        # reconnecting slskd silently discarded half a cohort while the
        # unit still exited 0) is NOT this check's job — the oneshot's own
        # bounded per-probe submit retry and circuit breaker handle that,
        # and the unit itself fails (non-zero exit) when the breaker trips
        # so an in-run outage is visible the same way a pre-run one is.
        ExecStartPre = lib.optional cfg.healthCheck.enable "+${slskdHealthCheck}";
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${unfindableDetectionPkg}/bin/cratedigger-unfindable";
        WorkingDirectory = cfg.stateDir;
        # Generous cap. Each candidate is bounded by roughly a ~30s
        # baseline search cycle plus, on a 409 (issue #1090), up to 2
        # submit retries -- each retry's own bounded backoff (<=5s,
        # PROBE_SUBMIT_RETRY_BACKOFF_S) plus a short dedicated server-
        # readiness timeout (SLSKD_SERVER_READINESS_TIMEOUT_S, a few
        # seconds, NOT this client's full HTTP timeout) -- worst case
        # comfortably under 60s/candidate. A sustained outage doesn't run
        # the full batch anyway: the circuit breaker stops it after 3
        # consecutive submit failures.
        #
        # Recomputed for DEFAULT_BATCH_SIZE=240 (issue #1112 item 1,
        # 2026-08-12; was 100/"2h"). Live per-probe wall time over the
        # last five daily runs (journalctl, 2026-08-08..12): 2026-08-08
        # 59.2s, 2026-08-09 56.0s, 2026-08-10 52.5s -- clean, healthy runs
        # -- versus 2026-08-11 43.1s (23/100 probe_failed) and 2026-08-12
        # 27.6s (the #1090 409-storm, 50/100 probe_failed), both partially
        # or fully degraded by this comment's own criterion: a failed
        # probe fails fast without a full search cycle, so more failures
        # pull the average down and understate true per-probe cost.
        # Excluding both degraded runs, healthy nominal is ~52-59s/probe
        # -> a K=240 batch costs ~3.5-4h. 5h keeps ~1.25x headroom over
        # that range -- comparable to the headroom the prior 2h/~98min
        # pairing held -- while still surfacing genuinely stuck runs well
        # inside the 24h daily cadence.
        TimeoutStartSec = "5h";
      };
    };

    systemd.timers.cratedigger-unfindable = {
      description = "Cratedigger unfindable detection daily timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        # Jitter the daily fire so the detection batch does not
        # collide with other midnight tasks on doc2 (logrotate,
        # postgres autovacuum, etc.). Single-operator install — there
        # is no fleet of NixOS deployments to spread across, the
        # randomisation is purely a local cron-collision avoidance.
        RandomizedDelaySec = "30min";
      };
    };

    # Daily whole-library retag-divergence census oneshot (#1142) — see
    # retagDivergenceCensusPkg above and lib/retag_divergence_audit.py's
    # own module docstring. Deliberately its own timer-driven unit,
    # mirroring cratedigger-unfindable: the ~200s whole-library scan must
    # never run on the 5-min main loop or at dashboard-render/API-request
    # time (lib/retag_divergence_census_snapshot.py is the read-only
    # persistence boundary the dashboard and CLI both read instead).
    # Beets-only, so — unlike cratedigger-unfindable — this unit has no
    # cratedigger-db-migrate.service dependency at all.
    systemd.services.cratedigger-retag-census = {
      description = "Cratedigger daily retag-divergence census oneshot";
      after = beetsReadinessUnits;
      wants = beetsReadinessUnits;
      restartIfChanged = false;
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        # #1142: the runner calls enforce_beets_startup(role="web") like
        # cratedigger-web/cratedigger-import-preview-worker, so it owes the
        # same read-only state-file bind those observer-role units use
        # (beetsObserverReadOnlyPaths) — otherwise the host-writable
        # cratedigger-ops group permission on state.pickle trips the
        # startup contract's state_writable_by_reader check. No
        # ProtectSystem/ReadWritePaths sandbox: unlike the CD-SEC-04 units
        # above, this oneshot accepts no untrusted network/media input, and
        # it still needs plain host write access to publish its snapshot
        # into cfg.stateDir.
        BindReadOnlyPaths = beetsObserverReadOnlyPaths;
        ExecStart = "${retagDivergenceCensusPkg}/bin/cratedigger-retag-census";
        WorkingDirectory = cfg.stateDir;
        # A measured live unbounded whole-library scan (~93,700 files)
        # took ~196s; generous headroom for a slower night or a larger
        # library, matching cratedigger-unfindable's own
        # measured-cost-plus-headroom convention above.
        TimeoutStartSec = "30min";
      };
    };

    systemd.timers.cratedigger-retag-census = {
      description = "Cratedigger daily retag-divergence census timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        # Same jitter rationale as cratedigger-unfindable's timer above —
        # avoid colliding with other midnight tasks on doc2.
        RandomizedDelaySec = "30min";
      };
    };

    systemd.services.cratedigger-library-completeness-census = {
      description = "Cratedigger daily whole-library completeness census";
      after = beetsReadinessUnits;
      wants = beetsReadinessUnits;
      restartIfChanged = false;
      serviceConfig = {
        Type = "oneshot";
        User = cfg.user;
        Group = cfg.group;
        BindReadOnlyPaths = beetsObserverReadOnlyPaths;
        # The trigger file is the operator "run census now" request (web
        # button / `pipeline-cli library-census-refresh`), watched by the
        # same-named path unit below. Removing it FIRST means a request
        # arriving DURING a run re-triggers after deactivation, so the
        # next snapshot reflects post-request state; a completed run
        # leaves no file and no re-trigger.
        ExecStartPre = "${pkgs.coreutils}/bin/rm -f ${cfg.stateDir}/library-completeness-census.trigger";
        ExecStart = "${libraryCompletenessCensusPkg}/bin/cratedigger-library-completeness-census";
        WorkingDirectory = cfg.stateDir;
        # Public MusicBrainz remains a supported (1 request/sec) posture:
        # a full 8k-release source census alone needs roughly 2.25h there.
        # The classifier has bounded concurrent workers and mirror clients
        # enforce their own semaphores, but public etiquette is deliberately
        # serial, so leave measured headroom instead of guaranteeing timeout.
        TimeoutStartSec = "4h";
      };
    };

    systemd.timers.cratedigger-library-completeness-census = {
      description = "Cratedigger daily library completeness census timer";
      wantedBy = ["timers.target"];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        RandomizedDelaySec = "30min";
      };
    };

    # Operator-forced census runs: the web service (same user) writes the
    # trigger file into stateDir; this path unit starts the same-named
    # oneshot. No sudo/polkit surface — file ownership IS the authority,
    # and the census service's ExecStart stays the single execution path.
    systemd.paths.cratedigger-library-completeness-census = {
      description = "Cratedigger library completeness census on-demand trigger";
      wantedBy = ["multi-user.target"];
      pathConfig = {
        PathExists = "${cfg.stateDir}/library-completeness-census.trigger";
      };
    };

    systemd.services.cratedigger-importer = mkIf cfg.importer.enable {
      description = "Cratedigger importer queue worker";
      after = ["cratedigger-db-migrate.service"] ++ beetsReadinessUnits;
      requires = ["cratedigger-db-migrate.service"] ++ beetsReadinessUnits;
      wantedBy = ["multi-user.target"];
      # Restart on deploy. The previous "skip restart to avoid killing
      # in-flight work" rationale failed in practice on 2026-05-16:
      # switch-to-configuration SIGTERM'd both workers anyway (units
      # changed transitively) and never brought them back, leaving the
      # pipeline silently dead for ~96 minutes. ``Restart=on-failure``
      # doesn't help — SIGTERM is a clean exit. The import-job launch fence
      # safely requeues only pre-launch work and stops ambiguous Beets work
      # for the operator; that's the right place to handle a mid-job kill,
      # not by leaving the worker dead.
      #
      # Issue #1089: the importer now catches this same ordinary deploy
      # SIGTERM and stops claiming new jobs, letting its own in-flight job
      # finish. That drain is only real with ``KillMode = "mixed"``: the
      # systemd DEFAULT (``control-group``) delivers SIGTERM to every
      # process in the unit's cgroup at once, including the beets child
      # subprocess — which has no handler and dies immediately regardless
      # of what the parent does (the RCA's own <1s stop). ``mixed`` signals
      # only the main PID; the child is untouched by the graceful stop and
      # can genuinely finish. ``TimeoutStopSec`` bounds how long systemd
      # waits for that drain before ``mixed``'s own escalation — a
      # cgroup-wide SIGKILL that takes the still-running child down with
      # the parent. A large virtiofs import can still exceed this bound
      # (the #1089 RCA's own 51-track box set ran 26 minutes); that
      # residual SIGKILL-mid-import world lands the owner process itself
      # dead, which is exactly the abandoned-owner recovery sweep
      # (`recover_abandoned_automation_owners`) and its recovery-side
      # crash-debris removal (`lib.automation_recovery_debris`) already
      # cover — a bounded wait here is deliberately preferred over an
      # unbounded one. The SAME debris check is also wired into the
      # in-process self-heal path (`_self_heal_automation_world_failure`)
      # for every OTHER child-death-with-surviving-parent world (OOM, a
      # crash, an operator `kill -9` of just the child) that this
      # KillMode change does not touch.
      restartIfChanged = true;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = (untrustedInputSandbox importerSandboxWritePaths) // {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        BindReadOnlyPaths = beetsConfigReadOnlyPaths ++ localImportReadOnlyPaths;
        BindPaths = missingOkExternalPath cfg.beets.runtime.expectedStateFile;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${importerPkg}/bin/cratedigger-importer";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
        KillMode = "mixed";
        TimeoutStopSec = "10min";
      };
    };

    systemd.services.cratedigger-import-preview-worker = mkIf cfg.importer.enable {
      description = "Cratedigger async import preview worker";
      after = ["cratedigger-db-migrate.service"] ++ beetsReadinessUnits;
      requires = ["cratedigger-db-migrate.service"] ++ beetsReadinessUnits;
      wantedBy = ["multi-user.target"];
      # Restart on deploy. Same reasoning as cratedigger-importer: deploy
      # SIGTERM'd this unit on 2026-05-16 and never brought it back.
      # ``requeue_stale_import_preview_jobs`` handles mid-job kills at startup;
      # leaving the worker dead instead is strictly worse.
      restartIfChanged = true;
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = (untrustedInputSandbox previewWorkerSandboxWritePaths) // {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        BindReadOnlyPaths = beetsObserverReadOnlyPaths ++ localImportReadOnlyPaths;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${previewWorkerPkg}/bin/cratedigger-import-preview-worker";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    # YouTube-rescue ingest drainer. Long-lived Type=simple worker that
    # polls `download_log` rows where source='youtube' and
    # outcome='youtube_running' that the operator explicitly opted in via
    # `pipeline-cli youtube-rescue <id>` or
    # POST /api/pipeline/<id>/youtube-rescue, invokes yt-dlp, stages audio
    # under the configured auto-import staging directory, and enqueues a
    # `youtube_import` row in `import_jobs` for the existing
    # cratedigger-importer worker to drain.
    #
    # Advisory-lock contention exits the process with code 0 (not 1), so
    # `Restart=on-failure` won't fire on duplicate-start. A genuine crash
    # (DB unreachable at boot, etc.) exits 1 and systemd will respawn after
    # `RestartSec=5`. There is NO `RuntimeMaxSec` — this is a long-running
    # daemon and the per-job yt-dlp timeout lives inside the worker
    # (DEFAULT_YTDLP_TIMEOUT_SEC = 600s). See
    # `docs/solutions/runtimemaxsec-vs-type-oneshot-systemd-incompatibility.md`.
    systemd.services.cratedigger-youtube-ingest = mkIf cfg.youtubeIngest.enable {
      description = "Cratedigger YouTube-rescue ingest worker";
      after = ["cratedigger-db-migrate.service"];
      requires = ["cratedigger-db-migrate.service"];
      wantedBy = ["multi-user.target"];
      # Deliberate `restartIfChanged = true`: deploy MUST pick up worker
      # code changes. Accepted-but-unclaimed `youtube_running` rows survive
      # restart and remain drainable; rows claimed by a previous worker are
      # swept to terminal `youtube_failed` on startup. Mirrors the importer /
      # preview-worker restart posture (2026-05-16 lesson).
      restartIfChanged = true;
      # Worker-specific PATH is set inside the wrapper (yt-dlp is
      # prepended there). The unit's `path` mirrors the importer's so
      # subprocess invocations have the standard toolchain available.
      path = [pkgs.bash pkgs.coreutils pkgs.gnugrep pkgs.gnused pkgs.curl pkgs.jq pkgs.ffmpeg pkgs.mp3val pkgs.flac pkgs.sox];
      serviceConfig = (untrustedInputSandbox youtubeIngestSandboxWritePaths) // {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        UMask = "0000";
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
        ExecStart = "${youtubeIngestWorkerPkg}/bin/cratedigger-youtube-ingest";
        WorkingDirectory = cfg.stateDir;
        Restart = "on-failure";
        RestartSec = 5;
      };
    };

    systemd.services.cratedigger-web = mkIf cfg.web.enable {
      description = "Cratedigger web UI";
      after = [
        "cratedigger-db-migrate.service"
        "cratedigger-web.socket"
      ] ++ redisServiceUnits ++ beetsReadinessUnits;
      wants = redisServiceUnits;
      requires = [
        "cratedigger-db-migrate.service"
        "cratedigger-web.socket"
      ] ++ beetsReadinessUnits;
      wantedBy = ["multi-user.target"];
      serviceConfig = (untrustedInputSandbox webSandboxWritePaths) // {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;
        SupplementaryGroups = [cfg.web.accessGroup];
        ExecStartPre =
          optional webBasicEnabled (
            "+${webBasicAuthValidationScript} "
            + lib.escapeShellArg webBasicAuthConfiguredPath
          )
          ++ optional
            webBasicEnabled
            webApplicationCredentialIsolationScript;
        BindReadOnlyPaths = beetsObserverReadOnlyPaths ++ localImportReadOnlyPaths;
        ExecStart = "${webPkg}/bin/cratedigger-web";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = "PIPELINE_DB_DSN=${pipelineDsn}";
      };
    };
  };
}
