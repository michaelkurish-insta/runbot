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
    from runbase.reconcile.matcher import find_strava_match
    from runbase.reconcile.enricher import enrich_from_strava

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

    prefix = "[DRY RUN] " if args.dry_run else ""
    print(f"\n{prefix}Reconcile complete:")
    print(f"  Matched:      {matched}")
    print(f"  Shoes set:    {shoes_set}")
    print(f"  Names set:    {names_set}")
    print(f"  Categories:   {categories_set}")

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
    reconcile_parser.add_argument("--dry-run", action="store_true", help="Show matches without writing")
    reconcile_parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    reconcile_parser.set_defaults(func=cmd_reconcile)

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
    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()
        sys.exit(1)
