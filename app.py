import os, json, re
from flask import Flask, render_template, request, jsonify
import duckdb
import pandas as pd

app = Flask(__name__)
DATA_DIR = "data"

os.makedirs(DATA_DIR, exist_ok=True)

def load_tables(con):
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            name = f[:-4]
            con.execute(f"""
                CREATE OR REPLACE TABLE {name} AS
                SELECT * FROM read_csv_auto('{DATA_DIR}/{f}');
            """)

@app.route("/")
def index():
    csvs = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    return render_template("index.html", csvs=csvs)

@app.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("files")
    for f in files:
        path = os.path.join(DATA_DIR, f.filename)
        f.save(path)
    return jsonify({"status": "ok"})

@app.route("/run", methods=["POST"])
def run():
    raw = request.form["raw"]

    # Determine whether input is JSON or direct SQL
    sql = raw
    try:
        temp = raw
        if temp.startswith('"') and temp.endswith('"'):
            temp = temp[1:-1]
        obj = json.loads(temp.encode().decode("unicode_escape"))
        sql = obj["command"].encode().decode("unicode_escape")
    except:
        pass

    # DuckDB connection
    con = duckdb.connect()

    # Load all CSVs as tables
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            name = f[:-4]
            con.execute(f"""
                CREATE OR REPLACE TABLE {name} AS
                SELECT * FROM read_csv_auto('{DATA_DIR}/{f}');
            """)

    # Execute SQL
    try:
        df = con.execute(sql).fetchdf()
        output = df.to_string(index=False)
    except Exception as e:
        output = str(e)

    # Export all tables back to CSV
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    for t in tables:
        out_path = f"{DATA_DIR}/{t}.csv"
        con.execute(f"COPY {t} TO '{out_path}' (HEADER, DELIMITER ',');")

    return jsonify({"sql": sql, "output": output})


@app.route("/reset", methods=["POST"])
def reset():
    for f in os.listdir(DATA_DIR):
        if f.endswith(".csv"):
            os.remove(os.path.join(DATA_DIR, f))
    return jsonify({"status": "ok"})


@app.get("/tables")
def list_tables():
    files = []
    for f in os.listdir("data"):
        if f.endswith(".csv"):
            table = f[:-4]
            files.append({"file": f, "table": table})
    return jsonify(files)



if __name__ == "__main__":
    app.run(debug=True)
