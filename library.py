import contextlib
import csv
import io
import itertools
import logging
import lzma
import os
import shlex
import sqlite3
import time
from datetime import datetime as dt
from ntpath import join
from pathlib import Path
from threading import Thread

import requests
import wx

from .events import MessageEvent, ResetGaugeEvent, UpdateGaugeEvent
from .helpers import PLUGIN_PATH, natural_sort_collation

class LZMAIterReader:
    '''Pop blocks of binary data from an iterator and de-LZMA it, implement an efficient readline for csv.reader'''
    def __init__(self, iter):
        self.iter = iter
        self.chunk = []
        self.linebuf = b''
        self.lzmad = lzma.LZMADecompressor()

    def read(self):
        if len(self.chunk) == 0:
            data = next(self.iter)
            self.chunk = self.lzmad.decompress(data)

        n = len(self.chunk)

        amt = min(n, len(self.chunk))
        rtn = self.chunk[0:amt]
        self.chunk = self.chunk[amt:]

        return rtn

    def __iter__(self):
        return self

    def __next__(self):
        return self.readline()

    def readline(self):
        while True:
            idx = self.linebuf.find(b'\r\n')
            if idx != -1:
                break

            newdata = self.read()
            if len(newdata) == 0:
                raise StopIteration()
            self.linebuf += newdata

        # Include \r\n in result
        idx += 2
        rtn = self.linebuf[0:idx]
        self.linebuf = self.linebuf[idx:]
        return rtn

class Library:
    """A storage class to get data from a sqlite database and write it back"""
    #CSV_URL = "https://yaqwsx.github.io/jlcparts/data/parts.csv.xz"
    CSV_URL = "https://www.dons.net.au/~darius/test.csv.xz"

    def __init__(self, parent):
        self.logger = logging.getLogger(__name__)
        self.parent = parent
        self.order_by = "LCSC Part"
        self.order_dir = "ASC"
        self.datadir = os.path.join(PLUGIN_PATH, "jlcpcb")
        self.dbfile = os.path.join(self.datadir, "parts.db")
        self.setup()

    def setup(self):
        """Check if folders and database exist, setup if not"""
        if not os.path.isdir(self.datadir):
            self.logger.info(
                "Data directory 'jlcpcb' does not exist and will be created."
            )
            Path(self.datadir).mkdir(parents=True, exist_ok=True)
        if not os.path.isfile(self.dbfile):
            self.update()

    def set_order_by(self, n):
        """Set which value we want to order by when getting data from the database"""
        order_by = [
            "LCSC Part",
            "MFR.Part",
            "Package",
            "Solder Joint",
            "Library Type",
            "Manufacturer",
            "Description",
            "Price",
            "Stock",
        ]
        if self.order_by == order_by[n] and self.order_dir == "ASC":
            self.order_dir = "DESC"
        else:
            self.order_by = order_by[n]
            self.order_dir = "ASC"

    def search(self, parameters):
        """Search the database for parts that meet the given parameters."""
        columns = [
            "LCSC Part",
            "MFR.Part",
            "Package",
            "Solder Joint",
            "Library Type",
            "Manufacturer",
            "Description",
            "Price",
            "Stock",
        ]
        s = ",".join(f'"{c}"' for c in columns)
        query = f"SELECT {s} FROM parts WHERE "

        try:
            keywords = shlex.split(parameters["keyword"])
        except ValueError as e:
            wx.PostEvent(
                self.parent,
                MessageEvent(
                    title="Query error",
                    text=f"Unable to split keywords: {str(e)}",
                    style="error",
                ),
            )
            self.logger.error("Can't split keyword: %s", str(e))
            return

        keyword_columns = [
            "LCSC Part",
            "Description",
            "MFR.Part",
            "Package",
            "Manufacturer",
        ]
        query_chunks = []
        for kw in keywords:
            q = " OR ".join(f'"{c}" LIKE "%{kw}%"' for c in keyword_columns)
            query_chunks.append(f"({q})")

        if p := parameters["manufacturer"]:
            query_chunks.append(f'"Manufacturer" LIKE "{p}"')
        if p := parameters["package"]:
            query_chunks.append(f'"Package" LIKE "{p}"')
        if p := parameters["category"]:
            query_chunks.append(
                f'("First Category" LIKE "{p}" OR "Second Category" LIKE "{p}")'
            )
        if p := parameters["part_no"]:
            query_chunks.append(f'"MFR.Part" LIKE "{p}"')
        if p := parameters["solder_joints"]:
            query_chunks.append(f'"Solder Joint" LIKE "{p}"')

        library_types = []
        if parameters["basic"]:
            library_types.append('"Basic"')
        if parameters["extended"]:
            library_types.append('"Extended"')
        if library_types:
            query_chunks.append(f'"Library Type" IN ({",".join(library_types)})')
        if parameters["stock"]:
            query_chunks.append(f'"Stock" > "0"')

        if not query_chunks:
            return []

        query += " AND ".join(query_chunks)
        query += f' ORDER BY "{self.order_by}" COLLATE naturalsort {self.order_dir}'
        query += " LIMIT 1000"

        self.logger.info("Query: %s", query)
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            con.create_collation("naturalsort", natural_sort_collation)
            with con as cur:
                return cur.execute(query).fetchall()

    def delete_parts_table(self):
        """Delete the parts table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(f"DROP TABLE IF EXISTS parts")
                cur.commit()

    def create_rotation_table(self):
        """Create the rotation table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS rotation ('regex', 'correction')"
                )
                cur.commit()

    def get_correction_data(self, regex):
        """Get the correction data by its regex."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                return cur.execute(
                    f"SELECT * FROM rotation WHERE regex = '{regex}'"
                ).fetchone()

    def delete_correction_data(self, regex):
        """Delete a correction from the database."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(f"DELETE FROM rotation WHERE regex = '{regex}'")
                cur.commit()

    def update_correction_data(self, regex, rotation):
        """Update a correction in the database."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    f"UPDATE rotation SET correction = '{rotation}' WHERE regex = '{regex}'"
                )
                cur.commit()

    def insert_correction_data(self, regex, rotation):
        """Insert a correction into the database."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cur.execute(
                    f"INSERT INTO rotation VALUES (?, ?)",
                    (regex, rotation),
                )
                cur.commit()

    def get_all_correction_data(self):
        """get all corrections from the database."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                return [
                    list(c)
                    for c in cur.execute(
                        f"SELECT * FROM rotation ORDER BY regex ASC"
                    ).fetchall()
                ]

    def create_parts_table(self, columns):
        """Create the parts table."""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                cols = ",".join([f" '{c}'" for c in columns])
                cur.execute(f"CREATE TABLE IF NOT EXISTS parts ({cols})")
                cur.commit()

    def get_stock(self, lcsc):
        """Get the stock for a given lcsc number"""
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            with con as cur:
                return cur.execute(
                    f'SELECT Stock FROM parts where "LCSC Part" = "{lcsc}"'
                ).fetchone()

    def update(self):
        """Update the sqlite parts database from the JLCPCB CSV."""
        Thread(target = self.download).start()

    def download(self):
        """The actual worker thread that downloads and imports the CSV data."""
        start = time.time()
        wx.PostEvent(self.parent, ResetGaugeEvent())
        r = requests.get(self.CSV_URL, allow_redirects=True, stream=True)
        if r.status_code != requests.codes.ok:
            wx.PostEvent(
                self.parent,
                MessageEvent(
                    title="Download Error",
                    text=f"Failed to download the JLCPCB database CSV, error code {r.status_code}",
                    style="error",
                ),
            )
            return

        size = r.headers.get("Content-Length")
        if size is None:
            wx.PostEvent(
                self.parent,
                MessageEvent(
                    title="Download Error",
                    text=f"Failed to download the JLCPCB database CSV, unable to determine size",
                    style="error",
                ),
            )
            return

        size = int(size)
        self.logger.info("Size %d", size)
        if size < 1000:
            wx.PostEvent(
                self.parent,
                MessageEvent(
                    title="Download Error",
                    text=f"Failed to download the JLCPCB database CSV, file too small ({size} bytes)",
                    style="error",
                ),
            )
        lastmod = r.headers.get("Last-Modified")
        self.logger.debug(
            f"Downloading file of size {(size / 1024 / 1024):.2f}MB, last modified {lastmod}"
        )

        l = LZMAIterReader(r.iter_content(chunk_size=65536))
        #l = lzma.LZMAFile(io.StringIO(r.content))
        csv_reader = csv.reader(map(lambda x: x.decode('utf-8'), l))
        headers = next(csv_reader)
        self.delete_parts_table()
        self.create_parts_table(headers)
        self.create_rotation_table()
        buffer = []
        with contextlib.closing(sqlite3.connect(self.dbfile)) as con:
            cols = ",".join(["?"] * len(headers))
            query = f"INSERT INTO parts VALUES ({cols})"

            for count, row in enumerate(csv_reader):
                buffer.append(row)
                if count % 1000 == 0:
                    self.logger.info("Count %d", count)
                    progress = r.raw.tell() / size * 100
                    wx.PostEvent(self.parent, UpdateGaugeEvent(value=progress))
                    con.executemany(query, buffer)
                    buffer = []
            if buffer:
                con.executemany(query, buffer)
            con.commit()
        self.logger.info("Done")
        wx.PostEvent(self.parent, ResetGaugeEvent())
        self.update_stock()
        wx.PostEvent(self.parent, ResetGaugeEvent())
        end = time.time()
        wx.PostEvent(
            self.parent,
            MessageEvent(
                title="Success",
                text=f"Sucessfully downloaded and imported the JLCPCB database in {end-start:.2f} seconds!",
                style="info",
            ),
        )

    def update_stock(self):
        """Update the stock info in the project from the library"""
        footprints = [fp for fp in self.parent.store.read_all() if fp[3]]
        self.logger.info(f"Update stock values for {len(footprints)} footprints")
        for n, fp in enumerate(footprints):
            progress = n / len(footprints) * 100
            if stock := self.get_stock(fp[3]):
                self.parent.store.set_stock(fp[0], stock[0])
                wx.PostEvent(self.parent, UpdateGaugeEvent(value=progress))
