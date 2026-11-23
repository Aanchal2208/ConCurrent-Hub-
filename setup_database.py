"""Configure the local PostgreSQL database for ConcurrentHub."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import psycopg
from dotenv import dotenv_values
from psycopg import sql

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"


def prompt_for_password() -> str:
    """Ask for the PostgreSQL password in a visible, masked Windows dialog."""
    import tkinter as tk
    from tkinter import ttk

    root = tk.Tk()
    root.title("ConcurrentHub database setup")
    root.resizable(False, False)
    root.attributes("-topmost", True)

    password = tk.StringVar()
    result: list[str] = []

    ttk.Label(
        root,
        text="Enter the PostgreSQL password you chose during installation:",
        padding=(16, 16, 16, 8),
    ).grid(row=0, column=0, columnspan=2, sticky="w")
    entry = ttk.Entry(root, textvariable=password, show="*", width=42)
    entry.grid(row=1, column=0, columnspan=2, padx=16, pady=(0, 12), sticky="ew")

    def submit() -> None:
        result.append(password.get())
        root.destroy()

    def cancel() -> None:
        root.destroy()

    ttk.Button(root, text="Cancel", command=cancel).grid(
        row=2, column=0, padx=(16, 6), pady=(0, 16), sticky="e"
    )
    ttk.Button(root, text="Continue", command=submit).grid(
        row=2, column=1, padx=(6, 16), pady=(0, 16), sticky="w"
    )
    root.bind("<Return>", lambda _event: submit())
    root.protocol("WM_DELETE_WINDOW", cancel)
    root.update_idletasks()
    x = (root.winfo_screenwidth() - root.winfo_width()) // 2
    y = (root.winfo_screenheight() - root.winfo_height()) // 2
    root.geometry(f"+{x}+{y}")
    entry.focus_set()
    root.mainloop()

    return result[0] if result else ""


def build_database_url(database_url: str, password: str) -> str:
    """Replace the URL password, safely escaping reserved characters."""
    parsed = urlsplit(database_url)
    if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname:
        raise ValueError("DATABASE_URL must be a PostgreSQL URL with a hostname.")

    username = unquote(parsed.username or "postgres")
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    port = parsed.port or 5432
    netloc = f"{quote(username, safe='')}:{quote(password, safe='')}@{host}:{port}"
    return urlunsplit(parsed._replace(netloc=netloc))


def save_database_url(database_url: str) -> None:
    """Update DATABASE_URL in the project's .env without changing other settings."""
    source = ENV_FILE if ENV_FILE.exists() else ENV_EXAMPLE
    lines = source.read_text(encoding="utf-8").splitlines()
    for index, line in enumerate(lines):
        if line.strip().startswith("DATABASE_URL="):
            lines[index] = f"DATABASE_URL={database_url}"
            break
    else:
        lines.insert(0, f"DATABASE_URL={database_url}")

    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    config_path = ENV_FILE if ENV_FILE.exists() else ENV_EXAMPLE
    config = dotenv_values(config_path)
    configured_url = config.get("DATABASE_URL")
    if not configured_url:
        print(f"DATABASE_URL is missing from {ENV_FILE}.")
        return 1

    try:
        parsed = urlsplit(configured_url)
        if parsed.scheme not in ("postgres", "postgresql") or not parsed.hostname:
            raise ValueError("DATABASE_URL must be a PostgreSQL URL with a hostname.")
        database_name = unquote(parsed.path.lstrip("/"))
        if not database_name:
            raise ValueError("DATABASE_URL must include a database name.")
        username = unquote(parsed.username or "postgres")
        print("A password window will open. Type there; characters appear as dots.")
        password = prompt_for_password()
        if not password:
            print("No password entered; setup cancelled.")
            return 1

        connection_args = {
            "host": parsed.hostname,
            "port": parsed.port or 5432,
            "user": username,
            "password": password,
            "connect_timeout": 5,
        }
        with psycopg.connect(
            dbname="postgres",
            autocommit=True,
            **connection_args,
        ) as connection:
            exists = connection.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (database_name,),
            ).fetchone()
            if not exists:
                connection.execute(
                    sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database_name))
                )

        database_url = build_database_url(configured_url, password)
        with psycopg.connect(database_url, connect_timeout=5) as connection:
            connection.execute("SELECT 1")
        save_database_url(database_url)
    except (psycopg.Error, ValueError, OSError) as exc:
        print(f"Database setup failed: {exc}")
        print(
            "Check that PostgreSQL is running and re-enter the exact password "
            "you chose in its installer. The password is not displayed."
        )
        return 1

    print(f"Database '{database_name}' is ready and .env has been updated.")
    print(r"Now run .\start.ps1 from the project folder.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
