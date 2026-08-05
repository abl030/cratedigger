# Cratedigger behind an identity provider you already run.
# =========================================================
#
# This is a sample, not a supported product. Copy it and adapt hostnames,
# ports, and your provider's endpoints.
#
# It shows the third web authorization mode: `web.externalAuth = true`, which
# declares that a component in front of the gateway owns the whole-site allow
# or deny decision. Cratedigger performs no authorization here. It sends no
# sub-request to your authorizer and does not probe whether one is reachable,
# so this option is an assertion you make about your own deployment. If the
# proxy below fails open, the UI is served anonymously and Cratedigger cannot
# tell.
#
# The example is written against nginx `auth_request` and a generic
# forward-auth endpoint. Authelia, authentik's proxy outpost, oauth2-proxy, and
# Pocket ID behind oauth2-proxy all present that shape; only the endpoint paths
# and the redirect URL differ. Caddy's `forward_auth` and Traefik's
# ForwardAuth middleware are the same contract in another syntax.
#
# Nothing here is Cratedigger-specific except the `proxy_pass` target: the
# module deliberately knows nothing about your provider.
{...}: let
  hostName = "music.example.com";
  gatewayPort = 8086;

  # Your identity provider. Both values are yours, not Cratedigger's.
  authorizerUpstream = "http://127.0.0.1:9091";
  portalUrl = "https://auth.example.com";
in {
  services.cratedigger = {
    enable = true;

    # ... the rest of your Cratedigger configuration; see cratedigger.nix ...

    web = {
      enable = true;
      inherit hostName gatewayPort;
      accessGroup = "cratedigger-web";

      # Exactly one mode. Setting this alongside basicAuthFile or
      # enableInsecure fails evaluation; there is no fallback between modes.
      externalAuth = true;
    };
  };

  # Your public vhost. Cratedigger's own gateway listens on loopback only, so
  # this proxy is necessarily in front of it and there is no path around your
  # authorizer that does not already require host access.
  services.nginx = {
    enable = true;
    # The module requires this so an authorization-policy change fails closed
    # without stopping unrelated virtual hosts.
    enableReload = true;

    virtualHosts.${hostName} = {
      forceSSL = true;
      enableACME = true;

      locations."/" = {
        extraConfig = ''
          auth_request /internal/authz;

          # Send the browser to your portal when the session is absent or
          # expired, and bring it back afterwards. This redirect is entirely
          # between the browser, this proxy, and your provider; Cratedigger
          # never participates in it.
          error_page 401 = @portal;

          proxy_http_version 1.1;
          proxy_pass http://127.0.0.1:${toString gatewayPort};

          # Required: the module gateway serves exactly this canonical host and
          # rejects anything else with a default vhost.
          proxy_set_header Host ${hostName};
          proxy_set_header Connection "";
        '';
      };

      # The forward-auth endpoint. It receives the browser's cookies, decides,
      # and answers 2xx or 401. Your provider's session cookie only ever has to
      # reach THIS proxy — whether it is scoped to this host or to a shared
      # parent domain is your provider's configuration, and Cratedigger imposes
      # no constraint on it.
      locations."/internal/authz" = {
        extraConfig = ''
          internal;
          proxy_pass ${authorizerUpstream}/api/authz/auth-request;
          proxy_pass_request_body off;
          proxy_set_header Content-Length "";
          proxy_set_header X-Original-URL $scheme://$http_host$request_uri;
          proxy_set_header X-Original-Method $request_method;
        '';
      };

      locations."@portal" = {
        extraConfig = ''
          return 302 ${portalUrl}/?rd=$scheme://$http_host$request_uri;
        '';
      };
    };
  };

  # Identity headers your authorizer sets — Remote-User, Remote-Groups,
  # Remote-Email, and your provider's session cookie — do not need to be
  # stripped here. Cratedigger's gateway rebuilds a reviewed header set before
  # the application, so none of them reach it in any mode. The module VM proves
  # this against a front proxy that injects them deliberately.
  #
  # Operator CLI authority is unaffected and stays outside this perimeter: it
  # is non-interactive Unix-socket access via the web access group.
  #
  #   users.users.your-operator.extraGroups = [ "cratedigger-web" ];

  # Anonymous health is your decision at this layer. Cratedigger keeps an exact
  # bodyless GET/HEAD /healthz exception on its own gateway; if you want an
  # unauthenticated monitor to reach it through this proxy, exempt it here:
  #
  #   locations."= /healthz".extraConfig = ''
  #     proxy_pass http://127.0.0.1:${toString gatewayPort};
  #     proxy_set_header Host ${hostName};
  #   '';
  #
  # Leaving it out means monitoring authenticates like everything else.
}
