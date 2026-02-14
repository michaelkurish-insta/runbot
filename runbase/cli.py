import argparse
import sys


def cmd_db_init(args):
    from runbase.db import init_db
    from runbase.config import load_config

    try:
        config = load_config()
    except FileNotFoundError:
        config = None
    init_db(config)


def cmd_sync(args):
    from runbase.config import load_config

    config = load_config()

    if not args.icloud and not args.strava:
        print("No sync source specified. Use --icloud or --strava.")
        sys.exit(1)

    if args.icloud:
        from runbase.ingest.icloud_sync import sync_icloud

        result = sync_icloud(config, dry_run=args.dry_run, verbose=args.verbose)
        _print_sync_summary(result, dry_run=args.dry_run)

    if args.strava:
        from runbase.ingest.strava_sync import sync_strava

        result = sync_strava(
            config,
            dry_run=args.dry_run,
            verbose=args.verbose,
            full_history=args.full_history,
            fetch_streams=not args.no_streams,
        )
        _print_strava_summary(result, dry_run=args.dry_run)


def _print_sync_summary(result: dict, dry_run: bool = False):
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Sync complete:")
    print(f"  New:      {result['new']}")
    print(f"  Skipped:  {result['skipped']}")
    print(f"  Errors:   {result['errors']}")
    if result.get("enriched"):
        print(f"  Enriched: {result['enriched']}")
    if result.get("interval_enriched"):
        print(f"  Intervals: {result['interval_enriched']}")

    if result["errors"] > 0:
        print("\nErrors:")
        for d in result["details"]:
            if d["status"] == "error":
                print(f"  {d['file']}: {d['error']}")


def _print_strava_summary(result: dict, dry_run: bool = False):
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}Strava sync complete:")
    print(f"  Matched:       {result['matched']}")
    print(f"  Unmatched:     {result['unmatched']}")
    print(f"  Skipped:       {result['skipped']}")
    print(f"  Errors:        {result['errors']}")
    print(f"  Fields filled: {result['fields_filled']}")
    print(f"  Laps inserted: {result['laps_inserted']}")
    print(f"  Streams pts:   {result['streams_inserted']}")
    print(f"  Shoes created: {result['shoes_created']}")

    if result["rate_limit_pauses"] > 0:
        print(f"  Rate pauses:   {result['rate_limit_pauses']}")

    if result["errors"] > 0:
        print("\nErrors:")
        for d in result["details"]:
            if d["status"] == "error":
                print(f"  strava:{d['strava_id']}: {d['error']}")


def cmd_import(args):
    from runbase.config import load_config

    config = load_config()

    if not args.xlsx:
        print("No import source specified. Use --xlsx.")
        sys.exit(1)

    from runbase.ingest.xlsx_import import import_xlsx

    result = import_xlsx(config, dry_run=args.dry_run, verbose=args.verbose)
    _print_import_summary(result, dry_run=args.dry_run)


def _print_import_summary(result: dict, dry_run: bool = False):
    prefix = "[DRY RUN] " if dry_run else ""

    if result.get("already_imported"):
        print(f"\n{prefix}XLSX already imported (file hash match). Nothing to do.")
        return

    print(f"\n{prefix}Import complete:")
    print(f"  New:              {result['new']}")
    print(f"  Skipped:          {result['skipped']}")
    print(f"  Errors:           {result['errors']}")
    print(f"  Non-running:      {result['skipped_non_running']}")

    stats = result.get("parse_stats", {})
    if stats:
        print(f"\n  Parse breakdown:")
        for method, count in sorted(stats.items()):
            print(f"    {method}: {count}")


def cmd_reconcile(args):
    from runbase.config import load_config
    from runbase.db import get_connection

    config = load_config()
    conn = get_connection(config)

    # Step 1: Backfill dates if requested
    if args.backfill_dates:
        from runbase.reconcile.matcher import backfill_strava_dates
        updated = backfill_strava_dates(config, conn, verbose=args.verbose)
        print(f"\nBackfill complete: {updated} orphaned Strava sources updated with dates.")
        if not args.dry_run:
            # If only backfilling, we can stop here unless there's also matching to do
            pass

    # Step 2: Find activities without a linked Strava source
    from runbase.reconcile.matcher import find_strava_match, find_strava_group_match
    from runbase.reconcile.enricher import enrich_from_strava, enrich_group_from_strava

    rows = conn.execute(
        """SELECT a.id, a.date, a.distance_mi
           FROM activities a
           WHERE NOT EXISTS (
               SELECT 1 FROM activity_sources s
               WHERE s.activity_id = a.id AND s.source = 'strava'
           )
           ORDER BY a.date"""
    ).fetchall()

    if args.verbose:
        print(f"\nFound {len(rows)} activities without a linked Strava source.")

    matched = 0
    shoes_set = 0
    names_set = 0
    categories_set = 0

    for r in rows:
        activity_id, date, distance_mi = r
        match = find_strava_match(conn, date, distance_mi)
        if not match:
            continue

        strava_name = match.get("strava_name", "")
        if args.verbose:
            print(f"  MATCH activity #{activity_id} ({date}, {distance_mi:.2f}mi) "
                  f'← Strava "{strava_name}"')

        if not args.dry_run:
            result = enrich_from_strava(conn, activity_id, match, verbose=args.verbose)
            conn.commit()
            if result["shoe_set"]:
                shoes_set += 1
            if result["name_set"]:
                names_set += 1
            if result["category_set"]:
                categories_set += 1

        matched += 1

    # Step 3: Group matching pass — multi-activity days
    # Re-query unlinked activities (some may have been matched in step 2)
    unlinked_rows = conn.execute(
        """SELECT a.id, a.date, a.distance_mi
           FROM activities a
           WHERE NOT EXISTS (
               SELECT 1 FROM activity_sources s
               WHERE s.activity_id = a.id AND s.source = 'strava'
           )
           ORDER BY a.date"""
    ).fetchall()

    if args.verbose:
        print(f"\nGroup matching: {len(unlinked_rows)} activities still unlinked.")

    group_matched = 0
    stream_fetch_pairs = []  # (strava_id, activity_id) for stream backfill

    for r in unlinked_rows:
        activity_id, date, distance_mi = r
        if distance_mi is None or distance_mi <= 0:
            continue

        group = find_strava_group_match(conn, date, distance_mi)
        if not group:
            continue

        group_names = [g.get("strava_name") or g.get("workout_name") or "?" for g in group]
        group_dists = [f"{g['distance_mi']:.2f}" for g in group]
        total = sum(g["distance_mi"] for g in group)
        if args.verbose:
            print(f"  GROUP activity #{activity_id} ({date}, {distance_mi:.2f}mi) "
                  f"← {len(group)} orphans ({' + '.join(group_dists)} = {total:.2f}mi): "
                  f"{', '.join(group_names)}")

        if not args.dry_run:
            result = enrich_group_from_strava(conn, activity_id, group, verbose=args.verbose)
            conn.commit()
            if result["shoe_set"]:
                shoes_set += 1
            if result["name_set"]:
                names_set += 1
            if result["category_set"]:
                categories_set += 1

            # Collect strava IDs for stream fetch
            for orphan in group:
                strava_id = orphan["source_id"]
                stream_fetch_pairs.append((strava_id, activity_id))

        group_matched += 1

    # Step 4: Fetch streams/laps for newly linked orphans (opt-in)
    stream_result = {"streams_inserted": 0, "laps_inserted": 0, "errors": 0, "rate_limit_pauses": 0}
    if args.fetch_streams and stream_fetch_pairs and not args.dry_run:
        from runbase.ingest.strava_sync import backfill_orphan_streams

        if args.verbose:
            print(f"\nFetching streams/laps for {len(stream_fetch_pairs)} orphan(s)...")
        stream_result = backfill_orphan_streams(
            config, conn, stream_fetch_pairs, verbose=args.verbose)

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Reconcile complete:")
    print(f"  1:1 matched:    {matched}")
    print(f"  Group matched:  {group_matched}")
    print(f"  Shoes set:      {shoes_set}")
    print(f"  Names set:      {names_set}")
    print(f"  Categories:     {categories_set}")
    if args.fetch_streams:
        print(f"  Streams pts:    {stream_result['streams_inserted']}")
        print(f"  Laps inserted:  {stream_result['laps_inserted']}")
        if stream_result["errors"]:
            print(f"  Fetch errors:   {stream_result['errors']}")
        if stream_result["rate_limit_pauses"]:
            print(f"  Rate pauses:    {stream_result['rate_limit_pauses']}")

    conn.close()


def cmd_vdot(args):
    from runbase.config import load_config
    from runbase.db import get_connection, _migrate_schema
    from runbase.analysis.vdot import (
        race_to_vdot, vdot_to_paces, set_vdot, get_current_vdot, format_pace,
    )

    config = load_config()
    conn = get_connection(config)
    _migrate_schema(conn)

    if args.set_value is not None:
        from datetime import date
        effective = args.date or date.today().isoformat()
        set_vdot(conn, args.set_value, effective, source="manual",
                 notes=args.notes)
        print(f"VDOT set to {args.set_value} (effective {effective})")

    elif args.from_race is not None:
        activity_id = args.from_race
        row = conn.execute(
            "SELECT date, distance_mi, duration_s FROM activities WHERE id = ?",
            (activity_id,),
        ).fetchone()
        if not row:
            print(f"Activity #{activity_id} not found.")
            sys.exit(1)
        act_date, dist_mi, dur_s = row
        if not dist_mi or not dur_s:
            print(f"Activity #{activity_id} missing distance or duration.")
            sys.exit(1)

        dist_m = dist_mi * 1609.344
        vdot = race_to_vdot(dist_m, dur_s)
        effective = args.date or act_date
        set_vdot(conn, vdot, effective, source="race", activity_id=activity_id,
                 notes=args.notes)
        pace_display = format_pace(dur_s / dist_mi)
        print(f"Race: {dist_mi:.2f}mi in {int(dur_s // 60)}:{int(dur_s % 60):02d} ({pace_display}/mi)")
        print(f"VDOT: {vdot} (effective {effective})")

    else:
        # Show current VDOT + training paces
        from datetime import date
        today = date.today().isoformat()
        vdot = get_current_vdot(conn, today)
        if not vdot:
            print("No VDOT set. Use 'vdot --set <value>' or 'vdot --from-race <activity_id>'.")
            conn.close()
            return

        paces = vdot_to_paces(vdot)
        print(f"Current VDOT: {vdot}")
        print(f"\nTraining paces:")
        for zone in ("E", "M", "T", "I", "R", "FR"):
            pace = paces[zone]
            print(f"  {zone:3s}  {format_pace(pace)}/mi")

        # Show VDOT history
        rows = conn.execute(
            "SELECT effective_date, vdot, source, notes FROM vdot_history ORDER BY effective_date DESC LIMIT 5"
        ).fetchall()
        if rows:
            print(f"\nRecent VDOT history:")
            for r in rows:
                note = f" ({r[3]})" if r[3] else ""
                print(f"  {r[0]}  VDOT {r[1]}  [{r[2]}]{note}")

    conn.close()


def cmd_enrich(args):
    from runbase.config import load_config
    from runbase.db import get_connection, _migrate_schema

    config = load_config()
    conn = get_connection(config)
    _migrate_schema(conn)

    from runbase.analysis.interval_enricher import enrich_activity, enrich_batch

    if args.activity:
        result = enrich_activity(conn, args.activity, config, verbose=args.verbose)
        if result["skipped"]:
            print(f"Skipped: {result['skip_reason']}")
        else:
            print(f"\nEnrichment complete for activity #{args.activity}:")
            print(f"  Track intervals:    {result['track_intervals']}")
            print(f"  Measured intervals: {result['measured_intervals']}")
            print(f"  Walking intervals:  {result['walking_intervals']}")
            print(f"  Stride intervals:   {result['stride_intervals']}")
            print(f"  Zones assigned:     {result['zones_assigned']}")
            if result["segments_created"]:
                print(f"  Segments created:   {result['segments_created']}")
    else:
        result = enrich_batch(conn, config, dry_run=args.dry_run, verbose=args.verbose)
        prefix = "[DRY RUN] " if args.dry_run else ""
        print(f"\n{prefix}Batch enrichment complete:")
        print(f"  Total activities:   {result['total']}")
        print(f"  Enriched:           {result['enriched']}")
        print(f"  Skipped:            {result['skipped']}")
        print(f"  Track intervals:    {result['track_intervals']}")
        print(f"  Measured intervals: {result['measured_intervals']}")
        print(f"  Walking intervals:  {result['walking_intervals']}")
        print(f"  Stride intervals:   {result['stride_intervals']}")
        print(f"  Zones assigned:     {result['zones_assigned']}")
        print(f"  Segments created:   {result['segments_created']}")

    conn.close()


def cmd_analyze_locations(args):
    from runbase.config import load_config
    from runbase.db import get_connection, _migrate_schema

    config = load_config()
    conn = get_connection(config)
    _migrate_schema(conn)

    from runbase.analysis.locations import cluster_workout_locations

    clusters = cluster_workout_locations(conn)

    if not clusters:
        print("No workout location clusters found.")
        print("(Need activities with intervals and GPS stream data.)")
        conn.close()
        return

    for i, cluster in enumerate(clusters):
        lat, lon = cluster["center_lat"], cluster["center_lon"]
        count = cluster["count"]
        print(f"\nCluster {chr(65 + i)} ({lat:.4f}, {lon:.4f}) — {count} workout(s):")
        for act in cluster["activities"]:
            name = act["name"] or "(unnamed)"
            print(f"  {act['date']}  \"{name}\"")

    print("\nTo mark a location as a measured course, add to config.yaml:")
    print("  paces:")
    print("    measured_courses:")
    print("      - name: \"Course Name\"")
    print("        lat: <lat>")
    print("        lon: <lon>")
    print("        radius_m: 500")

    conn.close()


def cmd_stub(name):
    def handler(args):
        print(f"'{name}' is not yet implemented.")
    return handler


def main():
    parser = argparse.ArgumentParser(prog="runbase", description="RunBase — running data pipeline")
    subparsers = parser.add_subparsers(dest="command")

    # db subcommand with its own subcommands
    db_parser = subparsers.add_parser("db", help="Database operations")
    db_sub = db_parser.add_subparsers(dest="db_command")
    db_init = db_sub.add_parser("init", help="Initialize the database schema")
    db_init.set_defaults(func=cmd_db_init)

    # sync subcommand
    sync_parser = subparsers.add_parser("sync", help="Sync data from sources")
    sync_parser.add_argument("--icloud", action="store_true", help="Sync from iCloud HealthFit folder")
    sync_parser.add_argument("--strava", action="store_true", help="Sync from Strava API")
    sync_parser.add_argument("--full-history", action="store_true", help="Ignore last sync timestamp, fetch everything")
    sync_parser.add_argument("--no-streams", action="store_true", help="Skip per-second stream data (faster sync)")
    sync_parser.add_argument("--dry-run", action="store_true", help="Show what would be imported without writing")
    sync_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    sync_parser.set_defaults(func=cmd_sync)

    # import subcommand
    import_parser = subparsers.add_parser("import", help="Import historical data")
    import_parser.add_argument("--xlsx", action="store_true", help="Import from training_log.xlsx")
    import_parser.add_argument("--dry-run", action="store_true", help="Show what would be imported without writing")
    import_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    import_parser.set_defaults(func=cmd_import)

    reconcile_parser = subparsers.add_parser("reconcile", help="Reconcile activities across sources")
    reconcile_parser.add_argument("--backfill-dates", action="store_true",
                                  help="Backfill start_date on orphaned Strava sources (one-time, requires API)")
    reconcile_parser.add_argument("--fetch-streams", action="store_true",
                                  help="Fetch streams/laps from Strava for newly group-matched orphans")
    reconcile_parser.add_argument("--dry-run", action="store_true", help="Show matches without writing")
    reconcile_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    reconcile_parser.set_defaults(func=cmd_reconcile)

    # vdot subcommand
    vdot_parser = subparsers.add_parser("vdot", help="Manage VDOT and training paces")
    vdot_parser.add_argument("--set", type=float, dest="set_value",
                             help="Set VDOT manually (e.g. --set 50)")
    vdot_parser.add_argument("--from-race", type=int, metavar="ACTIVITY_ID",
                             help="Calculate VDOT from a race activity")
    vdot_parser.add_argument("--date", type=str,
                             help="Effective date (default: today or race date)")
    vdot_parser.add_argument("--notes", type=str, help="Notes for this VDOT entry")
    vdot_parser.set_defaults(func=cmd_vdot)

    # enrich subcommand
    enrich_parser = subparsers.add_parser("enrich", help="Enrich intervals with pace zones, track detection, etc.")
    enrich_parser.add_argument("--activity", type=int, metavar="ID",
                               help="Enrich a single activity by ID")
    enrich_parser.add_argument("--dry-run", action="store_true",
                               help="Show what would be enriched without writing")
    enrich_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    enrich_parser.set_defaults(func=cmd_enrich)

    # analyze subcommand with sub-subcommands
    analyze_parser = subparsers.add_parser("analyze", help="Analysis tools")
    analyze_sub = analyze_parser.add_subparsers(dest="analyze_command")
    locations_parser = analyze_sub.add_parser("locations", help="Show workout location clusters")
    locations_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    locations_parser.set_defaults(func=cmd_analyze_locations)

    review_parser = subparsers.add_parser("review", help="Launch the review UI")
    review_parser.set_defaults(func=cmd_stub("review"))

    status_parser = subparsers.add_parser("status", help="Show pipeline status")
    status_parser.set_defaults(func=cmd_stub("status"))

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    if args.command == "db" and not getattr(args, "db_command", None):
        db_parser.print_help()
        sys.exit(1)
    if args.command == "analyze" and not getattr(args, "analyze_command", None):
        analyze_parser.print_help()
        sys.exit(1)
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)
