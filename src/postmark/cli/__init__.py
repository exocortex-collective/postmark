"""Command-line interfaces for Postmark.

Main entry point for the `postmark` CLI command.
"""

import click

from postmark.cli.backfill import backfill


@click.group()
@click.version_option(package_name="postmark")
def main() -> None:
    """Postmark CLI - Gmail sync and email pipeline tools."""
    pass


# Register subcommands
main.add_command(backfill)

if __name__ == "__main__":
    main()
