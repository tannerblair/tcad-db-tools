import csv
import sqlite3 as db
from pathlib import Path


def create_table_from_layout_file(conn: db.Connection, prop_layout_file: Path):
    with prop_layout_file.open(encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        cols = []
        for row in csv.reader(file):
            if row[0] != 'filler':
                cols.append(f"{row[0]} {row[1]}")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {prop_layout_file.stem} ({', '.join(cols)});""")


if __name__ == '__main__':
    conn = db.connect('tcad2021.db')
    prop_layout_file = Path("layout-files/2020/PROP.csv")
    prop_file = Path("source-files/2020/PROP.TXT")
    create_table_from_layout_file(conn, prop_layout_file)
    fields = {}
    with prop_layout_file.open(encoding="utf-8-sig") as layout_file:
        reader = csv.reader(layout_file)
        for row in csv.reader(layout_file):
            if row[0] != 'filler':
                fields[row[0]] = {}
                fields[row[0]]['start'] = int(row[2])-1
                fields[row[0]]['end'] = int(row[3])

    with prop_file.open() as file:
        lines = file.readlines()
        entries = []
        idx = 0
        for line in lines:
            entry = {}
            for name, value in fields.items():
                val_string = line[value['start']:value['end']].strip()
                val_string = val_string.replace('"', "'")
                entry[name] = f'"{val_string}"'
            exec_string = f"INSERT INTO PROP ({', '.join(entry.keys())}) VALUES ({', '.join(entry.values())})"
            conn.execute(exec_string)
            idx += 1
            if not idx % 1000:
                print(idx)
    conn.commit()
