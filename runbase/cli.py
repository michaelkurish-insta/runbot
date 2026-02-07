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

    # Stub subcommands for future phases
    sync_parser = subparsers.add_parser("sync", help="Sync data from sources")
    sync_parser.set_defaults(func=cmd_stub("sync"))

    import_parser = subparsers.add_parser("import", help="Import historical data")
    import_parser.set_defaults(func=cmd_stub("import"))

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
