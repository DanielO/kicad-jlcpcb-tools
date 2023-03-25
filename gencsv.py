#!/usr/bin/env python3

import argparse
import csv
import io
import json
import lzma
import sqlite3
import sys


def gencsv(dbname, csvname):
    dbh = sqlite3.connect(dbname)
    dbh.row_factory = sqlite3.Row
    c = dbh.cursor()

    lzmaf = lzma.LZMAFile(csvname, "w")
    csvf = csv.writer(io.TextIOWrapper(lzmaf, "utf-8"))
    csvf.writerow(
        [
            "LCSC Part",
            "First Category",
            "Second Category",
            "MFR.Part",
            "Package",
            "Solder Joint",
            "Manufacturer",
            "Library Type",
            "Description",
            "Datasheet",
            "Price",
            "Stock",
        ]
    )

    c.execute(
        "select lcsc, category, subcategory, mfr, package, joints, manufacturer, basic, description, datasheet, stock, price from v_components"
    )
    for r in c:
        if r["basic"] == 0:
            libtype = "Basic"
        else:
            libtype = "Extended"
        j = json.loads(r["price"])
        prices = []
        for price in j:
            pricestr = ""
            if price["qFrom"]:
                pricestr = str(price["qFrom"])
            pricestr += "-"
            if price["qTo"]:
                pricestr += str(price["qTo"])

            pricestr += ":"
            pricestr += str(price["price"])
            prices.append(pricestr)
        pricestr = ",".join(prices)
        csvf.writerow(
            [
                "C" + str(r["lcsc"]),
                r["category"],
                r["subcategory"],
                r["mfr"],
                r["package"],
                r["joints"],
                r["manufacturer"],
                libtype,
                r["description"],
                r["datasheet"],
                pricestr,
                r["stock"],
            ]
        )


def main():
    parser = argparse.ArgumentParser(
        description="Extract data from a JLCPCB cache database as an LZMA compressed CSV for use by the kicad-jlcpcb-tools plugin"
    )
    parser.add_argument("dbfile", help="SQLite database filename to extract CSV from")
    parser.add_argument("csvfile", help="LZMA compressed CSV filename to write data to")
    args = parser.parse_args()

    gencsv(args.dbfile, args.csvfile)


if __name__ == "__main__":
    main()
