import csv
import sqlite3 as db
from pathlib import Path



def create_db_table(conn: db.Connection, layout_file: Path):
    print(f"Creating table for {layout_file.stem.lower()}")
    with layout_file.open(encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        cols = []
        for row in csv.reader(file):
            if row[0] != 'filler':
                key = row[0].replace('/ ', '_').replace(' ', '_')
                cols.append(f"{key} {row[1]}")
        exec_string = f"""CREATE TABLE IF NOT EXISTS {layout_file.stem.lower()} ({', '.join(cols)});"""
        try:
            conn.execute(exec_string)
        except db.OperationalError as e:
            print(exec_string)
            raise db.OperationalError(e)


def get_field_info(layout_file: Path):
    fields = {}
    with layout_file.open(encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        for row in csv.reader(file):
            if row[0] != 'filler':
                key = row[0].replace('/ ', '_').replace(' ', '_')
                fields[key] = {}
                try:
                    fields[key]['start'] = int(row[2]) - 1
                    fields[key]['end'] = int(row[3])
                except ValueError as e:
                    print(fields)
                    raise ValueError(e)

    return fields


def add_rows_to_db(conn: db.Connection, layout_file: Path, data_file: Path):
    print(f"Adding rows for {layout_file}")
    fields = get_field_info(layout_file)
    with data_file.open() as file:
        lines = file.readlines()
        for line in lines:
            entry = {}
            for name, value in fields.items():
                val_string = line[value['start']:value['end']].strip()
                val_string = val_string.replace('"', "'")
                entry[name] = f'"{val_string}"'
            exec_string = f"INSERT INTO {layout_file.stem.lower()} ({', '.join(entry.keys())}) VALUES ({', '.join(entry.values())})"
            try:
                conn.execute(exec_string)
            except db.OperationalError as e:
                print(exec_string)
                raise db.OperationalError(e)


def add_table(conn: db.Connection, layout_file: Path, data_file: Path):
    print(f"Adding rows for {layout_file}")
    create_db_table(conn, layout_file)
    add_rows_to_db(conn, layout_file, data_file)


if __name__ == '__main__':
    print("Starting...")
    src_file_dir = Path("source-files/2020")
    layout_file_dir = Path("layout-files/2020")
    db2020 = db.connect('tcad-2020.db')
    for file in src_file_dir.iterdir():
        print(f"Processing {file}")
        layout = layout_file_dir / f"{file.stem}.CSV"
        add_table(db2020, layout, file)
    db2020.commit()
