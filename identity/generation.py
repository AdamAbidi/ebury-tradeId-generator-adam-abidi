# -*- coding: utf-8 -*-

import os
import sqlite3
import threading
from identity.constants import ID_CHARACTERS

DB_PATH = os.path.join(os.path.dirname(__file__), "identity.db")

_init_lock = threading.Lock()

def _init_conn():
    with _init_lock:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
        cur = conn.cursor()
        # pragmatic performance PRAGMAs for better write and read concurrency.
        cur.execute("PRAGMA journal_mode=WAL;") # readers don’t block while one writer is active
        cur.execute("PRAGMA synchronous=NORMAL;")

        # Single-row table: stores ONLY the latest code (7-char string). No history is kept.
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS trade_id_counter (
          code CHAR(7) NOT NULL UNIQUE
        );
        """)
        return conn


def encode(n: int):
    """Convert integer to 7-char Base-34 code, left-padded with '0'."""
    if n == 0:
        return "0000000"
    code = []
    while n:
        # remainder picks the digit (character index); quotient moves to next place
        r = n % len(ID_CHARACTERS)
        n = n // len(ID_CHARACTERS)
        code.append(ID_CHARACTERS[r])

    # Reverse because digits are appended in reverse order
    # Apply rjust to add "0" till the total length equal to 7
    return ''.join(reversed(code)).rjust(7, "0")


def decode(code: str):
    """Convert 7-char Base-34 code -> integer."""
    n = 0
    for ch in code:
        # multiply by base, then add current digit value (index in alphabet)
        n = n * len(ID_CHARACTERS) + ID_CHARACTERS.index(ch)
    return n


def generate():
    return create_ids(1)[0]


def generate_bulk(n: int):
    return create_ids(n)


def create_ids(n:int):
    """
        Core allocator:
          - If table empty: return '0000000'.. encode(n-1), and persist the last.
          - Otherwise: read latest, generate next N, persist the last.
        Persistence stores *only* the last code (one row total).
    """
    if n <= 0:
        return []
    # Requesting more than total space fails fast.
    if n > len(ID_CHARACTERS)**7:
        raise OverflowError("Space Limit Reached")

    conn = _init_conn()
    cur = conn.cursor()

    # short transaction to avoid races; cheap with WAL
    cur.execute("BEGIN IMMEDIATE")

    # read current (exactly one row)
    cur.execute("SELECT code FROM trade_id_counter LIMIT 1;")
    row = cur.fetchone()
    if row is None:
        # First-ever allocation: start at 0000000 (index 0), end at n-1.
        new_code = encode(n-1)

        # Persist only the last code of the batch.
        cur.execute("INSERT INTO trade_id_counter(code) VALUES (?);", (new_code,))
        cur.execute("COMMIT")

        # Return the whole batch: 0 .. n-1 encoded.
        return [encode(i) for i in range(0, n)]

    else:
        # Normal path: continue from the latest stored code.
        (current_code,) = row
        current_value = decode(current_code)
        new_value = current_value + n
        if new_value > len(ID_CHARACTERS) ** 7 - 1:
            raise OverflowError("Space Limit Reached")

        # Persist only the last of this batch.
        new_code = encode(new_value)
        cur.execute("UPDATE trade_id_counter SET code = ? WHERE code = ?;",
                     (new_code, current_code))
        cur.execute("COMMIT")

        # Return the batch: (current_value + 1) .. new_value
        return [encode(i) for i in range(current_value + 1, new_value + 1)]


# Note: In production, this function could be split into smaller helpers
# (init case, continuation case, batch generation) for clarity and testability.