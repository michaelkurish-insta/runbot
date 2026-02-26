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
        if args.backfill_laps:
            from runbase.ingest.strava_sync import backfill_strava_laps

            result = backfill_strava_laps(config, verbose=args.verbose)
            print(f"\nStrava lap backfill complete:")
            print(f"  Activities fetched: {result['fetched']}")
            print(f"  Skipped:            {result['skipped']}")
            print(f"  Errors:             {result['errors']}")
            if result["rate_limit_pauses"]:
                print(f"  Rate pauses:        {result['rate_limit_pauses']}")
            return

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

    # Step 4: Promote orphaned Strava sources to activities (opt-in)
    promoted_count = 0
    promoted_activities = []
    if args.promote_orphans:
        from runbase.reconcile.matcher import find_promotable_orphans
        from runbase.reconcile.enricher import promote_orphans

        if args.verbose:
            print(f"\nLooking for promotable orphans (since {args.promote_since})...")

        groups = find_promotable_orphans(conn, cutoff_date=args.promote_since,
                                         verbose=args.verbose)

        if groups:
            total_orphans = sum(len(g) for g in groups)
            if args.verbose:
                print(f"  Found {len(groups)} day(s) with {total_orphans} orphan(s) to promote.")

            if args.dry_run:
                for group in groups:
                    primary = group[0]
                    total_dist = sum(o["distance_mi"] or 0 for o in group)
                    names = [o.get("strava_name") or o.get("workout_name") or "?" for o in group]
                    if len(group) == 1:
                        print(f"  [DRY RUN] Would promote {primary['start_date']} "
                              f"{total_dist:.2f}mi \"{names[0]}\"")
                    else:
                        dists = [f"{o['distance_mi']:.2f}" for o in group]
                        print(f"  [DRY RUN] Would promote {primary['start_date']} "
                              f"{' + '.join(dists)} = {total_dist:.2f}mi ({', '.join(names)})")
                promoted_count = len(groups)
            else:
                promoted_activities = promote_orphans(conn, groups, verbose=args.verbose)
                promoted_count = len(promoted_activities)

                # Collect stream fetch pairs from promoted activities
                for p in promoted_activities:
                    stream_fetch_pairs.extend(p["pairs"])
        elif args.verbose:
            print("  No promotable orphans found.")

    # Step 5: Fetch streams/laps for newly linked orphans (opt-in)
    stream_result = {"streams_inserted": 0, "laps_inserted": 0, "errors": 0, "rate_limit_pauses": 0}
    if args.fetch_streams and stream_fetch_pairs and not args.dry_run:
        from runbase.ingest.strava_sync import backfill_orphan_streams

        if args.verbose:
            print(f"\nFetching streams/laps for {len(stream_fetch_pairs)} orphan(s)...")
        stream_result = backfill_orphan_streams(
            config, conn, stream_fetch_pairs, verbose=args.verbose)

    # Step 6: Enrich promoted activities (if streams were fetched)
    enriched_count = 0
    if promoted_activities and not args.dry_run:
        from runbase.analysis.interval_enricher import enrich_activity

        if args.verbose:
            print(f"\nEnriching {len(promoted_activities)} promoted activity/ies...")
        for p in promoted_activities:
            try:
                result = enrich_activity(conn, p["activity_id"], config, verbose=args.verbose)
                if not result["skipped"]:
                    enriched_count += 1
            except Exception as e:
                if args.verbose:
                    print(f"    ERROR enriching activity #{p['activity_id']}: {e}")

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Reconcile complete:")
    print(f"  1:1 matched:    {matched}")
    print(f"  Group matched:  {group_matched}")
    if args.promote_orphans:
        print(f"  Promoted:       {promoted_count}")
        if enriched_count:
            print(f"  Enriched:       {enriched_count}")
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


_DISTANCE_ALIASES = {
    "mile": 1609, "mi": 1609, "1mi": 1609,
    "5k": 5000, "10k": 10000,
    "half": 21097, "marathon": 42195,
}


def cmd_fastest(args):
    from runbase.config import load_config
    from runbase.db import get_connection
    from runbase.analysis.fastest import find_fastest

    dist_str = args.distance.lower()
    if dist_str in _DISTANCE_ALIASES:
        target_m = _DISTANCE_ALIASES[dist_str]
        dist_label = dist_str
    else:
        try:
            target_m = float(args.distance)
        except ValueError:
            print(f"Unknown distance: {args.distance}")
            sys.exit(1)
        dist_label = f"{target_m:g}m"

    config = load_config()
    conn = get_connection(config)
    results = find_fastest(conn, target_m, top_n=args.top, verbose=args.verbose)

    if not results:
        print(f"No {dist_label} segments found.")
        return

    print(f"\nTop {len(results)} fastest {dist_label}:\n")
    print(f"  {'#':>3}  {'Date':<12} {'Act#':>5}  {'Workout':<30} "
          f"{'Time':>8}  {'Pace':>9}  {'Source':<10}")
    print(f"  {'':->3}  {'':->12} {'':->5}  {'':->30} "
          f"{'':->8}  {'':->9}  {'':->10}")

    for i, r in enumerate(results, 1):
        secs = r["duration_s"]
        if secs < 60:
            time_str = f"{secs:5.1f}s"
        else:
            m, s = divmod(secs, 60)
            time_str = f"{int(m)}:{s:04.1f}"
        pace = r["pace_s_per_mi"]
        pace_str = f"{int(pace // 60)}:{pace % 60:04.1f}/mi"
        print(f"  {i:>3}  {r['date']:<12} #{r['activity_id']:>4}  "
              f"{r['workout_name']:<30} {time_str:>8}  {pace_str:>9}  "
              f"{r['source_type']:<10}")


def cmd_pipeline(args):
    """Run the full sync pipeline: iCloud → Strava → enrich new activities."""
    from runbase.config import load_config
    from runbase.db import get_connection, _migrate_schema

    config = load_config()
    verbose = args.verbose

    # Step 1: iCloud sync (FIT files)
    if verbose:
        print("=== iCloud sync ===")
    from runbase.ingest.icloud_sync import sync_icloud

    icloud_result = sync_icloud(config, verbose=verbose)
    new_activity_ids = [
        d["activity_id"] for d in icloud_result.get("details", [])
        if d.get("status") == "new" and d.get("activity_id")
    ]

    if verbose or icloud_result["new"]:
        print(f"  {icloud_result['new']} new, {icloud_result['skipped']} skipped")

    # Step 2: Strava sync
    if verbose:
        print("\n=== Strava sync ===")
    from runbase.ingest.strava_sync import sync_strava

    strava_result = sync_strava(config, verbose=verbose, fetch_streams=True)
    if verbose or strava_result["matched"]:
        print(f"  {strava_result['matched']} matched, {strava_result['fields_filled']} fields filled")

    # Step 2b: Lightweight reconcile — link orphaned Strava sources to activities
    if verbose:
        print("\n=== Reconcile ===")
    from runbase.reconcile.matcher import find_strava_match
    from runbase.reconcile.enricher import enrich_from_strava

    conn = get_connection(config)
    _migrate_schema(conn)
    rows = conn.execute(
        """SELECT a.id, a.date, a.distance_mi
           FROM activities a
           WHERE NOT EXISTS (
               SELECT 1 FROM activity_sources s
               WHERE s.activity_id = a.id AND s.source = 'strava'
           )
           ORDER BY a.date"""
    ).fetchall()

    reconciled_ids = []
    for r in rows:
        activity_id, date, distance_mi = r
        match = find_strava_match(conn, date, distance_mi)
        if not match:
            continue
        result = enrich_from_strava(conn, activity_id, match, verbose=verbose)
        conn.commit()
        reconciled_ids.append(activity_id)
        if verbose:
            strava_name = match.get("strava_name", "")
            print(f"  MATCH activity #{activity_id} ({date}, {distance_mi:.2f}mi) "
                  f'← Strava "{strava_name}"')

    if verbose or reconciled_ids:
        print(f"  {len(reconciled_ids)} reconciled from {len(rows)} unlinked")

    # Include reconciled activities in the enrichment pass
    enrich_ids = list(set(new_activity_ids + reconciled_ids))

    # Step 3: Enrich new + reconciled activities
    if enrich_ids:
        if verbose:
            print(f"\n=== Enriching {len(enrich_ids)} activities ===")
        from runbase.analysis.interval_enricher import enrich_activity

        for aid in enrich_ids:
            enrich_activity(conn, aid, config, verbose=verbose)
    elif verbose:
        print("\n  No new activities to enrich.")

    conn.close()

    # Summary
    print(f"\nPipeline complete: {icloud_result['new']} new, "
          f"{strava_result['matched']} Strava matched, "
          f"{len(reconciled_ids)} reconciled")


def cmd_review(args):
    from runbase.config import load_config
    from runbase.review.app import create_app

    config = load_config()
    app = create_app(config)
    print(f"RunBase Review UI: http://localhost:{args.port}")
    app.run(host="127.0.0.1", port=args.port, debug=args.debug)


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
    sync_parser.add_argument("--backfill-laps", action="store_true",
                             help="Fetch Strava laps for activities that already have XLSX intervals (one-time)")
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
    reconcile_parser.add_argument("--promote-orphans", action="store_true",
                                  help="Create activities from orphaned Strava sources that have no matching activity")
    reconcile_parser.add_argument("--promote-since", type=str, default="2025-12-01", metavar="YYYY-MM-DD",
                                  help="Cutoff date for orphan promotion (default: 2025-12-01)")
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

    fastest_parser = subparsers.add_parser(
        "fastest", help="Find fastest segments at a given distance")
    fastest_parser.add_argument(
        "distance",
        help="Target distance in meters (or alias: mile, 5k, 10k, half, marathon)")
    fastest_parser.add_argument(
        "-n", "--top", type=int, default=10,
        help="Number of results (default 10)")
    fastest_parser.add_argument("-v", "--verbose", action="store_true")
    fastest_parser.set_defaults(func=cmd_fastest)

    # pipeline subcommand (cron-friendly)
    pipeline_parser = subparsers.add_parser(
        "pipeline", help="Run full sync pipeline: iCloud → Strava → enrich")
    pipeline_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    pipeline_parser.set_defaults(func=cmd_pipeline)

    review_parser = subparsers.add_parser("review", help="Launch the review UI")
    review_parser.add_argument("-p", "--port", type=int, default=5050, help="Port (default 5050)")
    review_parser.add_argument("--debug", action="store_true", help="Enable Flask debug mode")
    review_parser.set_defaults(func=cmd_review)

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
