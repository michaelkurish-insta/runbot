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
    print(f"  New:     {result['new']}")
    print(f"  Skipped: {result['skipped']}")
    print(f"  Errors:  {result['errors']}")

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
    reconcile_parser.set_defaults(func=cmd_stub("reconcile"))

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
