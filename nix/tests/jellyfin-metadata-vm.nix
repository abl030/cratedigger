# Jellyfin 10.11 metadata-refresh acceptance test for the real Cratedigger
# post-import notifier. This deliberately lives outside module-vm.nix: the
# stranger-boot gate proves module wiring, while this gate boots the pinned
# media server and proves the external API contract against tagged audio.
{ pkgs, cratediggerSrc }:

let
  cratedigger = pkgs.callPackage ../package.nix { };
  pythonEnv = cratedigger.pythonEnv;
  emptyJson = pkgs.writeText "jellyfin-empty.json" "{}";
  authJson = pkgs.writeText "jellyfin-auth.json" (builtins.toJSON {
    Username = "jellyfin";
  });
  libraryOptionsJson = pkgs.writeText "jellyfin-library-options.json" (
    builtins.toJSON {
      LibraryOptions.EnableRealtimeMonitor = false;
    }
  );
in
assert pkgs.jellyfin.version == "10.11.11";
pkgs.testers.nixosTest {
  name = "cratedigger-jellyfin-metadata-vm";

  nodes.machine = { pkgs, ... }: {
    services.jellyfin.enable = true;
    services.postgresql = {
      enable = true;
      ensureDatabases = [ "root" ];
      ensureUsers = [{
        name = "root";
        ensureDBOwnership = true;
      }];
    };
    environment.systemPackages = with pkgs; [ curl ffmpeg postgresql ];

    # Jellyfin refuses startup below 2 GiB free. The fixture itself is tiny.
    virtualisation.diskSize = 3 * 1024;
    virtualisation.memorySize = 2048;
  };

  testScript = ''
    import json
    import shlex
    from urllib.parse import urlencode

    base_url = "http://127.0.0.1:8096"
    pipeline_dsn = "postgresql:///root?host=/run/postgresql"
    existing_path = "/srv/target/Legacy Artist/1999 - Existing Tagged Album"
    existing_track = existing_path + "/01.flac"
    replacement_track = existing_path + "/01.opus"
    client_auth = (
        'MediaBrowser Client="Cratedigger VM", DeviceId="metadata-vm", '
        'Device="NixOS", Version="1"'
    )

    def curl(path, *, token=None, method="GET", body=None):
        headers = ["-H", "X-Emby-Authorization:" + client_auth]
        if token is not None:
            headers = ["-H", "X-Emby-Token:" + token]
        command = ["curl", "--fail", "--silent", "--show-error"]
        command += headers
        if method != "GET":
            command += ["-X", method]
        if body is not None:
            command += ["-H", "Content-Type:application/json", "--data", "@" + body]
        command.append(base_url + path)
        return machine.succeed(" ".join(shlex.quote(part) for part in command))

    def get_json(path, *, token):
        return json.loads(curl(path, token=token))

    def make_audio(
        path, *, codec="flac", album, album_artist, title, year, genre,
        track, disc
    ):
        machine.succeed("install -d -m 0755 " + shlex.quote(path.rsplit("/", 1)[0]))
        command = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", "sine=frequency=440:duration=0.25",
            "-c:a", codec,
            "-metadata", "album=" + album,
            "-metadata", "album_artist=" + album_artist,
            # Mirror the two album-artist aliases present on real Beets Opus
            # files, rather than relying on ffmpeg's single default spelling.
            "-metadata", "ALBUM ARTIST=" + album_artist,
            "-metadata", "artist=" + album_artist,
            "-metadata", "title=" + title,
            "-metadata", "date=" + str(year),
            "-metadata", "genre=" + genre,
            "-metadata", "track=" + str(track),
            "-metadata", "disc=" + str(disc),
            path,
        ]
        machine.succeed(" ".join(shlex.quote(part) for part in command))

    def make_cover(path):
        command = [
            "ffmpeg", "-y", "-loglevel", "error",
            "-f", "lavfi", "-i", "color=c=navy:s=32x32",
            "-frames:v", "1", path,
        ]
        machine.succeed(" ".join(shlex.quote(part) for part in command))

    def run_cratedigger(script, *args):
        command = [
            "env", "PYTHONPATH=${cratediggerSrc}",
            "${pythonEnv}/bin/python", "-c", script, *args,
        ]
        return machine.succeed(" ".join(shlex.quote(part) for part in command))

    machine.start()
    machine.wait_for_unit("postgresql.service")
    migrate = [
        "env", "PYTHONPATH=${cratediggerSrc}",
        "${pythonEnv}/bin/python", "${cratediggerSrc}/scripts/migrate_db.py",
        "--dsn", pipeline_dsn,
        "--migrations-dir", "${cratediggerSrc}/migrations",
    ]
    machine.succeed(" ".join(shlex.quote(part) for part in migrate))
    machine.wait_for_unit("jellyfin.service")
    machine.wait_for_open_port(8096)
    machine.wait_until_succeeds(
        "journalctl --since -2m --unit jellyfin --grep 'Startup complete'"
    )
    machine.wait_until_succeeds("curl --fail --silent http://127.0.0.1:8096/health | grep Healthy")

    # Complete the first-run wizard and authenticate as the default admin.
    retry(lambda _: curl("/Startup/Configuration"))
    curl("/Startup/FirstUser")
    curl("/Startup/Complete", method="POST", body="${emptyJson}")
    auth = json.loads(curl(
        "/Users/AuthenticateByName", method="POST", body="${authJson}"
    ))
    token = auth["AccessToken"]
    user_id = get_json("/Users/Me", token=token)["Id"]

    # File-system change reports are intentionally debounced by Jellyfin.
    # Keep the real FileRefresher path, but shorten its test-only delay so the
    # VM proves convergence rather than spending a minute per notification.
    server_config = get_json("/System/Configuration", token=token)
    server_config["LibraryMonitorDelay"] = 1
    machine.succeed(
        "printf %s " + shlex.quote(json.dumps(server_config))
        + " > /tmp/jellyfin-server-config.json"
    )
    curl(
        "/System/Configuration",
        token=token,
        method="POST",
        body="/tmp/jellyfin-server-config.json",
    )

    # The existing album is indexed first, then manually curated through the
    # same full-DTO update contract Cratedigger's DateCreated pin uses.
    make_audio(
        existing_track,
        album="Existing Tagged Album",
        album_artist="Legacy Artist",
        title="Legacy Track",
        year=1999,
        genre="Legacy",
        track=1,
        disc=1,
    )
    make_cover(existing_path + "/cover.jpg")
    machine.succeed("install -d -m 0755 /srv/unrelated")

    target_query = urlencode({
        "name": "Target Music",
        "collectionType": "music",
        "paths": "/srv/target",
        "refreshLibrary": "true",
    })
    unrelated_query = urlencode({
        "name": "Unrelated Music",
        "collectionType": "music",
        "paths": "/srv/unrelated",
        "refreshLibrary": "false",
    })
    curl(
        "/Library/VirtualFolders?" + target_query,
        token=token,
        method="POST",
        body="${libraryOptionsJson}",
    )
    curl(
        "/Library/VirtualFolders?" + unrelated_query,
        token=token,
        method="POST",
        body="${libraryOptionsJson}",
    )

    folders = get_json("/Library/VirtualFolders", token=token)
    target_id = next(row["ItemId"] for row in folders if row["Name"] == "Target Music")
    unrelated_id = next(
        row["ItemId"] for row in folders if row["Name"] == "Unrelated Music"
    )

    def libraries_idle(_):
        current = get_json("/Library/VirtualFolders", token=token)
        return all(row.get("RefreshStatus") == "Idle" for row in current)

    def albums(parent_id):
        query = urlencode({
            "ParentId": parent_id,
            "Recursive": "true",
            "IncludeItemTypes": "MusicAlbum",
            "Fields": "Path,Genres,ProductionYear,AlbumArtists,Overview,LockData,ImageTags",
        })
        return get_json("/Items?" + query, token=token)["Items"]

    def audio_children(album_id):
        query = urlencode({
            "ParentId": album_id,
            "Recursive": "true",
            "IncludeItemTypes": "Audio",
            "Fields": "Album,IndexNumber,ParentIndexNumber,Path,DateCreated",
        })
        return get_json("/Items?" + query, token=token)["Items"]

    existing = {}

    def existing_landed(_):
        matches = [row for row in albums(target_id) if row["Name"] == "Existing Tagged Album"]
        if not matches:
            return False
        existing.update(matches[0])
        return True

    retry(existing_landed)
    retry(libraries_idle)

    # Full DTO update: this is a realistic curated item, not a synthetic
    # partial POST that Jellyfin would interpret as field deletion.
    existing_dto = get_json(
        "/Items/" + existing["Id"] + "?userId=" + user_id,
        token=token,
    )
    existing_image_tag = existing_dto.get("ImageTags", {}).get("Primary")
    assert existing_image_tag, existing_dto
    existing_dto["Name"] = "Curated Existing Name"
    existing_dto["Overview"] = "Operator-authored archival note"
    existing_dto["LockData"] = True
    machine.succeed(
        "printf %s " + shlex.quote(json.dumps(existing_dto))
        + " > /tmp/curated-existing.json"
    )
    curl(
        "/Items/" + existing["Id"],
        token=token,
        method="POST",
        body="/tmp/curated-existing.json",
    )

    # Add both a target fixture and an unrelated fixture only after initial
    # indexing. Realtime monitoring is disabled on both libraries, so only
    # an accidentally broad refresh can discover the unrelated album.
    make_audio(
        "/srv/target/Tagged Album Artist/2024 - Notifier Metadata Album/01.flac",
        album="Notifier Metadata Album",
        album_artist="Tagged Album Artist",
        title="First Track",
        year=2024,
        genre="Archival",
        track=1,
        disc=1,
    )
    make_audio(
        "/srv/target/Tagged Album Artist/2024 - Notifier Metadata Album/02.flac",
        album="Notifier Metadata Album",
        album_artist="Tagged Album Artist",
        title="Second Track",
        year=2024,
        genre="Archival",
        track=2,
        disc=1,
    )
    make_cover(
        "/srv/target/Tagged Album Artist/2024 - Notifier Metadata Album/cover.jpg"
    )
    make_audio(
        "/srv/unrelated/Other Artist/2023 - Unrelated Album/01.flac",
        album="Unrelated Album",
        album_artist="Other Artist",
        title="Other Track",
        year=2023,
        genre="Other",
        track=1,
        disc=1,
    )

    # Invoke the real Cratedigger notifier. HTTP 204 is only queueing
    # evidence; every assertion below polls Jellyfin's observable state.
    notifier = (
        "from lib.config import CratediggerConfig; "
        "from lib.util import trigger_jellyfin_scan; "
        "import sys; "
        "trigger_jellyfin_scan(CratediggerConfig("
        "beets_directory='/srv/target', "
        "jellyfin_url='http://127.0.0.1:8096', "
        "jellyfin_token=sys.argv[1]), sys.argv[2])"
    )
    run_cratedigger(
        notifier,
        token,
        "/srv/target/Tagged Album Artist/2024 - Notifier Metadata Album",
    )

    new_album = {}
    children = []

    def metadata_landed(_):
        matches = [
            row for row in albums(target_id)
            if row["Name"] == "Notifier Metadata Album"
        ]
        if not matches:
            return False
        candidate = matches[0]
        child_query = urlencode({
            "ParentId": candidate["Id"],
            "Recursive": "true",
            "IncludeItemTypes": "Audio",
            "Fields": "Album,IndexNumber,ParentIndexNumber",
        })
        observed_children = get_json("/Items?" + child_query, token=token)["Items"]
        observed_projection = [
            (row.get("Album"), row.get("IndexNumber"), row.get("ParentIndexNumber"))
            for row in sorted(
                observed_children,
                key=lambda row: row.get("IndexNumber", 0),
            )
        ]
        if observed_projection != [
            ("Notifier Metadata Album", 1, 1),
            ("Notifier Metadata Album", 2, 1),
        ]:
            return False
        if candidate.get("ProductionYear") != 2024:
            return False
        if "Archival" not in candidate.get("Genres", []):
            return False
        if [row["Name"] for row in candidate.get("AlbumArtists", [])] != [
            "Tagged Album Artist"
        ]:
            return False
        if not candidate.get("ImageTags", {}).get("Primary"):
            return False
        new_album.update(candidate)
        children[:] = observed_children
        return True

    retry(metadata_landed, timeout_seconds=120)
    retry(libraries_idle, timeout_seconds=120)

    assert new_album["Name"] == "Notifier Metadata Album", new_album
    album_artists = [row["Name"] for row in new_album.get("AlbumArtists", [])]
    assert album_artists == ["Tagged Album Artist"], new_album
    assert new_album.get("ProductionYear") == 2024, new_album
    assert "Archival" in new_album.get("Genres", []), new_album
    assert new_album.get("ImageTags", {}).get("Primary"), new_album

    child_projection = [
        (row.get("Album"), row.get("IndexNumber"), row.get("ParentIndexNumber"))
        for row in sorted(children, key=lambda row: row.get("IndexNumber", 0))
    ]
    assert child_projection == [
        ("Notifier Metadata Album", 1, 1),
        ("Notifier Metadata Album", 2, 1),
    ], children

    latest_query = urlencode({
        "ParentId": target_id,
        "IncludeItemTypes": "Audio",
        "GroupItems": "true",
        "Fields": "Album,AlbumArtists,Genres,ProductionYear",
        "Limit": "20",
    })
    latest = []

    def grouped_latest_landed(_):
        observed = get_json(
            "/Users/" + user_id + "/Items/Latest?" + latest_query,
            token=token,
        )
        latest[:] = observed
        return any(
            row.get("Type") == "MusicAlbum"
            and row.get("Name") == "Notifier Metadata Album"
            for row in observed
        )

    retry(grouped_latest_landed, timeout_seconds=120)
    assert not any(
        row.get("Type") == "Audio" and not row.get("Album") for row in latest
    ), latest

    # Scoped targeting: the unrelated fixture stays undiscovered.
    assert albums(unrelated_id) == [], albums(unrelated_id)

    curated = get_json(
        "/Items/" + existing["Id"] + "?userId=" + user_id,
        token=token,
    )
    assert curated["Name"] == "Curated Existing Name", curated
    assert curated.get("Overview") == "Operator-authored archival note", curated
    assert curated.get("ImageTags", {}).get("Primary") == existing_image_tag, curated

    # Production DateCreated pin lifecycle: stamp a deliberately old value
    # with the full-DTO setter, capture it into real PostgreSQL, replace the
    # track so Jellyfin must create a different Audio item, then reconcile
    # only after observable id drift and scan completion.
    existing_children = audio_children(existing["Id"])
    assert len(existing_children) == 1, existing_children
    snapshot_child_ids = {row["Id"] for row in existing_children}
    requested_old_date = "2001-02-03T04:05:06.0000000Z"
    set_date = (
        "import sys; "
        "from lib.config import CratediggerConfig; "
        "from lib.util import jellyfin_set_date_created; "
        "cfg=CratediggerConfig(jellyfin_url='http://127.0.0.1:8096', "
        "jellyfin_token=sys.argv[1]); "
        "assert all(jellyfin_set_date_created(cfg, item_id, sys.argv[2]) "
        "for item_id in sys.argv[3:])"
    )
    run_cratedigger(
        set_date,
        token,
        requested_old_date,
        existing["Id"],
        *sorted(snapshot_child_ids),
    )

    dated_album = get_json(
        "/Items/" + existing["Id"] + "?userId=" + user_id,
        token=token,
    )
    dated_children = audio_children(existing["Id"])
    captured_original = dated_album["DateCreated"]
    assert captured_original.startswith("2001-02-03T04:05:06"), dated_album
    assert {
        row["DateCreated"] for row in dated_children
    } == {captured_original}, dated_children

    capture_pin = (
        "import json,sys; "
        "from dataclasses import asdict; "
        "from lib.config import CratediggerConfig; "
        "from lib.jellyfin_pin_service import capture_jellyfin_date_created_pin; "
        "from lib.pipeline_db import PipelineDB; "
        "cfg=CratediggerConfig(beets_directory='/srv/target', "
        "jellyfin_url='http://127.0.0.1:8096', jellyfin_token=sys.argv[1]); "
        "db=PipelineDB(sys.argv[2]); "
        "result=capture_jellyfin_date_created_pin(cfg, db, sys.argv[3], None); "
        "print(json.dumps(asdict(result))); db.close()"
    )
    capture_result = json.loads(run_cratedigger(
        capture_pin,
        token,
        pipeline_dsn,
        existing_path,
    ))
    assert capture_result["outcome"] == "captured", capture_result
    assert capture_result["original_date_created"] == captured_original, capture_result
    pin_id = capture_result["pin_id"]

    staged_replacement = "/tmp/01.opus"
    make_audio(
        staged_replacement,
        codec="libopus",
        album="Existing Tagged Album",
        album_artist="Legacy Artist",
        title="Legacy Track Remastered",
        year=1999,
        genre="Legacy",
        track=1,
        disc=1,
    )
    # Beets exposes a completed file at its final library path.  Stage the
    # encoded file outside the watched tree so Jellyfin cannot ingest a
    # half-written Opus stream before Cratedigger reports the final path.
    machine.succeed(
        "mv " + shlex.quote(staged_replacement) + " "
        + shlex.quote(replacement_track)
    )
    machine.succeed("rm " + shlex.quote(existing_track))
    run_cratedigger(notifier, token, existing_path)

    current_existing = {}
    current_children = []

    def upgrade_landed(_):
        matches = [row for row in albums(target_id) if row.get("Path") == existing_path]
        if len(matches) != 1:
            return False
        observed_children = audio_children(matches[0]["Id"])
        observed_ids = {row["Id"] for row in observed_children}
        if len(observed_children) != 1 or observed_ids == snapshot_child_ids:
            return False
        if observed_children[0].get("Path") != replacement_track:
            return False
        current_existing.update(matches[0])
        current_children[:] = observed_children
        return True

    retry(upgrade_landed, timeout_seconds=120)
    retry(libraries_idle, timeout_seconds=120)
    assert {row["Id"] for row in current_children} != snapshot_child_ids

    reconcile_pin = (
        "import json,sys; "
        "from dataclasses import asdict; "
        "from datetime import datetime,timezone; "
        "from lib.config import CratediggerConfig; "
        "from lib.jellyfin_pin_service import reconcile_jellyfin_date_created_pins; "
        "from lib.pipeline_db import PipelineDB; "
        "cfg=CratediggerConfig(beets_directory='/srv/target', "
        "jellyfin_url='http://127.0.0.1:8096', jellyfin_token=sys.argv[1]); "
        "db=PipelineDB(sys.argv[2]); "
        "result=reconcile_jellyfin_date_created_pins("
        "cfg, db, now=datetime.now(timezone.utc), grace_seconds=0); "
        "print(json.dumps(asdict(result))); db.close()"
    )
    reconcile_result = json.loads(run_cratedigger(
        reconcile_pin,
        token,
        pipeline_dsn,
    ))
    assert reconcile_result == {
        "pinned": 1,
        "already_correct": 0,
        "waiting": 0,
        "skipped": 0,
        "expired": 0,
        "errors": 0,
    }, reconcile_result

    final_matches = [row for row in albums(target_id) if row.get("Path") == existing_path]
    assert len(final_matches) == 1, final_matches
    final_album = get_json(
        "/Items/" + final_matches[0]["Id"] + "?userId=" + user_id,
        token=token,
    )
    final_children = audio_children(final_matches[0]["Id"])
    assert final_album["Name"] == "Curated Existing Name", final_album
    assert final_album.get("Overview") == "Operator-authored archival note", final_album
    assert final_album["DateCreated"] == captured_original, final_album
    assert {
        row["DateCreated"] for row in final_children
    } == {captured_original}, final_children
    pin_status_sql = "SELECT status FROM jellyfin_date_created_pins WHERE id = " + str(pin_id)
    pin_status = machine.succeed(
        "psql " + shlex.quote(pipeline_dsn)
        + " -At -c " + shlex.quote(pin_status_sql)
    ).strip()
    assert pin_status == "done", pin_status
    assert albums(unrelated_id) == [], albums(unrelated_id)
  '';
}
