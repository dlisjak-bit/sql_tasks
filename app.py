import os
import json
import sqlite3
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_from_directory

app = Flask(
    __name__
)

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "database.sqlite")


############################
# HELPERS
############################

from flask import send_from_directory

@app.route("/csvview/<path:filename>")
def csv_viewer(filename):
    csv_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(csv_path):
        return f"CSV not found: {filename}", 404
    return render_template("csv_viewer.html", filename=filename)

@app.route("/csvraw/<path:filename>")
def csv_raw(filename):
    csv_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(csv_path):
        return "File not found", 404
    return send_from_directory(DATA_DIR, filename)





def load_tables(conn):
    """
    Load CSV files into SQLite tables, but only if they do not already
    exist. This preserves dropped tables (so CSVs do not get reloaded).
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    existing_tables = {row[0] for row in cursor.fetchall()}

    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            table = f[:-4]
            csv_path = os.path.join(DATA_DIR, f)

            if table not in existing_tables:
                df = pd.read_csv(csv_path)
                df.to_sql(table, conn, if_exists="replace", index=False)


def export_all_tables(conn):
    """
    Write all SQLite tables back to CSV.
    Also delete CSVs for tables that no longer exist.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]

    # Export SQLite tables → CSVs
    for t in tables:
        df = pd.read_sql_query(f"SELECT * FROM {t}", conn)
        out = os.path.join(DATA_DIR, f"{t}.csv")
        df.to_csv(out, index=False)

    # Delete CSVs for tables that were dropped
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            table = f[:-4]
            if table not in tables:
                os.remove(os.path.join(DATA_DIR, f))


############################
# FLASK ROUTES
############################

@app.route("/")
def index():
    csvs = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    return render_template("index.html", csvs=csvs)


@app.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("files")
    for f in files:
        f.save(os.path.join(DATA_DIR, f.filename))
    return jsonify({"status": "ok"})


@app.route("/run", methods=["POST"])
def run():
    raw = request.form["raw"]

    # Determine JSON {"command": "..."} or direct SQL
    sql = raw
    try:
        t = raw
        if t.startswith('"') and t.endswith('"'):
            t = t[1:-1]
        obj = json.loads(t.encode().decode("unicode_escape"))
        sql = obj["command"].encode().decode("unicode_escape")
    except:
        pass

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Load CSVs as tables
    load_tables(conn)

    def remove_sql_comments(text):
        lines = text.splitlines()
        new_lines = []
        in_block = False

        for line in lines:
            stripped = line.strip()

            # Block comment start
            if stripped.startswith("/*"):
                in_block = True
                continue

            # Block comment end
            if stripped.endswith("*/"):
                in_block = False
                continue

            # Inside block comment — skip
            if in_block:
                continue

            # Line comment
            if stripped.startswith("--"):
                continue

            new_lines.append(line)

        return "\n".join(new_lines)


    sql_no_comments = remove_sql_comments(sql)
    sql_clean = sql_no_comments.strip().rstrip(";")
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    last_stmt = statements[-1]


    try:
        # Case 1 — last statement is SELECT
        if last_stmt.lower().startswith("select"):
            # Execute all non-select statements
            if len(statements) > 1:
                non_select_block = ";".join(statements[:-1]) + ";"
                cursor.executescript(non_select_block)
                conn.commit()

            # Now run the SELECT and capture output
            df = pd.read_sql_query(last_stmt, conn)
            output = df.to_string(index=False)

        # Case 2 — no SELECT anywhere
        else:
            cursor.executescript(sql)
            conn.commit()
            output = "(Statement executed successfully)"

    except Exception as e:
        err = str(e)

        # Find approximate error location
        token = None
        if "near" in err:
            try:
                token = err.split("near")[1].split(":")[0].strip().strip('"')
            except:
                pass

        # Find line containing token
        error_line_num = None
        if token:
            for i, line in enumerate(sql_no_comments.splitlines(), start=1):
                if token in line:
                    error_line_num = i
                    break

        # Build readable error message
        formatted = ["ERROR:"]
        formatted.append(err)

        if error_line_num:
            formatted.append(f"\nAt line {error_line_num}:")
            formatted.append(sql_no_comments.splitlines()[error_line_num - 1])
            formatted.append(" " * (sql_no_comments.splitlines()[error_line_num - 1].find(token)) + "^")

        output = "\n".join(formatted)


    # Export updated tables → CSV, delete dropped ones
    export_all_tables(conn)
    conn.close()

    return jsonify({"sql": sql, "output": output})


@app.route("/reset", methods=["POST"])
def reset():
    # Remove CSVs
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            os.remove(os.path.join(DATA_DIR, f))

    # Remove SQLite database
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    return jsonify({"status": "ok"})


@app.get("/tables")
def list_tables():
    items = []
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            items.append({"file": f, "table": f[:-4]})
    return jsonify(items)


if __name__ == "__main__":
    app.run(debug=True)
