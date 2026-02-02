from flask import Flask, render_template, request, flash, redirect, url_for, Response, stream_with_context
from flask_bootstrap import Bootstrap5
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, SubmitField, IntegerField, SelectField
from wtforms.validators import DataRequired
import sqlalchemy
from sqlalchemy import create_engine, text
import urllib.parse
import sys
import subprocess
import os
from flask import jsonify
import time

import json

app = Flask(__name__)
app.secret_key = 'change_this_to_a_secure_secret_key'
bootstrap = Bootstrap5(app)

CONFIG_FILE = 'config.json'

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config: {e}")
    return {}

def save_config(data):
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"Error saving config: {e}")

@app.route('/browse_db', methods=['GET'])
def browse_db():
    try:
        # Use a separate process to open the file dialog to avoid thread issues with Flask
        # We use a simple python script executed via subprocess
        cmd = [
            sys.executable, 
            "-c", 
            "import tkinter; import tkinter.filedialog; root = tkinter.Tk(); root.withdraw(); root.attributes('-topmost', True); print(tkinter.filedialog.askopenfilename(filetypes=[('Firebird DB', '*.fdb'), ('All files', '*.*')]))"
        ]
        
        # Run the command and capture output
        result = subprocess.run(cmd, capture_output=True, text=True)
        file_path = result.stdout.strip()
        
        return jsonify({'path': file_path})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

class TransferForm(FlaskForm):
    # Source Database Config
    src_host = StringField('Source Host', default='localhost', validators=[DataRequired()])
    src_port = IntegerField('Source Port', default=3050, validators=[DataRequired()])
    src_path = StringField('Source DB Path (e.g., C:/data/source.fdb)', validators=[DataRequired()])
    # src_user and src_pass removed from form, will be hardcoded
    src_charset = SelectField('Source Charset', choices=[('WIN1251', 'WIN1251'), ('UTF8', 'UTF8'), ('NONE', 'NONE')], default='WIN1251')

    # Destination Database Config
    dst_host = StringField('Dest Host', default='localhost', validators=[DataRequired()])
    dst_port = IntegerField('Dest Port', default=3050, validators=[DataRequired()])
    dst_path = StringField('Dest DB Path (e.g., C:/data/dest.fdb)', validators=[DataRequired()])
    # dst_user and dst_pass removed from form, will be hardcoded
    dst_charset = SelectField('Dest Charset', choices=[('WIN1251', 'WIN1251'), ('UTF8', 'UTF8'), ('NONE', 'NONE')], default='WIN1251')

    # Transfer Config
    table_name = StringField('Table Name', default='EVENTS', validators=[DataRequired()], description="Name of the table to transfer")
    src_condition = StringField('Selection Condition (WHERE)', default="EVENTSDATE >= '01.01.2026'", description="SQL WHERE clause for source selection (e.g., ID > 100)")
    check_columns = StringField('Unique Columns for Check', default='READERID, EVENTSCODE, EVENTSDATE, CARDNUM', validators=[DataRequired()], description="Comma-separated columns to check for duplicates (e.g., ID, CODE)")

    submit = SubmitField('Start Transfer')

def get_db_uri(user, password, host, port, path, charset):
    # Construct Firebird URI
    # Note: password might need encoding if special chars exist
    safe_pass = urllib.parse.quote_plus(password)
    return f"firebird+fdb://{user}:{safe_pass}@{host}:{port}/{path}?charset={charset}"

@app.route('/', methods=['GET', 'POST'])
def index():
    config = load_config()
    form = TransferForm(data=config if request.method == 'GET' and config else None)
    
    # We remove the POST handling here to rely on the JS streaming implementation
    # But we keep the form object for rendering
    
    return render_template('index.html', form=form)

def transfer_worker(form_data):
    try:
        # Hardcoded credentials
        DEFAULT_USER = 'SYSDBA'
        DEFAULT_PASS = 'masterkey'

        # Helper to get data safely
        def get_val(key):
            return form_data.get(key)

        src_uri = get_db_uri(
            DEFAULT_USER, DEFAULT_PASS, 
            get_val('src_host'), get_val('src_port'), 
            get_val('src_path'), get_val('src_charset')
        )
        dst_uri = get_db_uri(
            DEFAULT_USER, DEFAULT_PASS, 
            get_val('dst_host'), get_val('dst_port'), 
            get_val('dst_path'), get_val('dst_charset')
        )

        yield json.dumps({"status": "info", "message": "Connecting to source database..."}) + "\n"
        
        src_engine = create_engine(src_uri, connect_args={'user': DEFAULT_USER, 'password': DEFAULT_PASS, 'charset': get_val('src_charset')})
        dst_engine = create_engine(dst_uri, connect_args={'user': DEFAULT_USER, 'password': DEFAULT_PASS, 'charset': get_val('dst_charset')})

        table = get_val('table_name')
        src_cond = get_val('src_condition')
        check_cols_str = get_val('check_columns')
        check_cols = [c.strip() for c in check_cols_str.split(',') if c.strip()]
        EXCLUDE_COLS = ['EVENTSID']

        if not check_cols:
            yield json.dumps({"status": "error", "message": "At least one unique column must be specified."}) + "\n"
            return

        inserted_count = 0
        skipped_count = 0

        # 1. Connect to Source and Select
        yield json.dumps({"status": "info", "message": "Fetching records from source..."}) + "\n"
        with src_engine.connect() as src_conn:
            select_sql = text(f"SELECT * FROM {table} WHERE {src_cond}")
            result = src_conn.execute(select_sql)
            src_rows = result.mappings().all()

        total_rows = len(src_rows)
        if total_rows == 0:
            yield json.dumps({"status": "completed", "message": "No records found in source database matching criteria.", "inserted": 0, "skipped": 0}) + "\n"
            return

        yield json.dumps({"status": "info", "message": f"Found {total_rows} records. Starting transfer...", "total": total_rows}) + "\n"

        # 2. Connect to Dest and Insert
        with dst_engine.connect() as dst_conn:
            trans = dst_conn.begin()
            try:
                for i, row in enumerate(src_rows):
                    row_data = {k.upper(): v for k, v in row.items()}
                    
                    check_clauses = [f"{col} = :check_{col}" for col in check_cols]
                    check_sql_str = f"SELECT count(*) FROM {table} WHERE {' AND '.join(check_clauses)}"
                    
                    check_params = {f"check_{col}": row_data[col.upper()] for col in check_cols}
                    
                    existing = dst_conn.execute(text(check_sql_str), check_params).scalar()
                    
                    if existing > 0:
                        skipped_count += 1
                    else:
                        cols = [k for k in row_data.keys() if k not in EXCLUDE_COLS]
                        col_list = ', '.join(cols)
                        val_placeholders = ', '.join([f":{c}" for c in cols])
                        
                        insert_sql = text(f"INSERT INTO {table} ({col_list}) VALUES ({val_placeholders})")
                        dst_conn.execute(insert_sql, row_data)
                        inserted_count += 1
                    
                    # Yield progress
                    if (i + 1) % 10 == 0 or (i + 1) == total_rows:
                        yield json.dumps({
                            "status": "progress", 
                            "current": i + 1, 
                            "total": total_rows, 
                            "inserted": inserted_count, 
                            "skipped": skipped_count
                        }) + "\n"
                
                trans.commit()
                yield json.dumps({
                    "status": "completed", 
                    "message": f"Transfer Complete. Inserted: {inserted_count}, Skipped: {skipped_count}",
                    "inserted": inserted_count, 
                    "skipped": skipped_count
                }) + "\n"
                
            except Exception as e:
                trans.rollback()
                raise e

    except Exception as e:
        yield json.dumps({"status": "error", "message": str(e)}) + "\n"

@app.route('/api/transfer', methods=['POST'])
def api_transfer():
    # Save config
    try:
        config_data = {
            'src_host': request.form.get('src_host'),
            'src_port': request.form.get('src_port'),
            'src_path': request.form.get('src_path'),
            'src_charset': request.form.get('src_charset'),
            'dst_host': request.form.get('dst_host'),
            'dst_port': request.form.get('dst_port'),
            'dst_path': request.form.get('dst_path'),
            'dst_charset': request.form.get('dst_charset'),
            'table_name': request.form.get('table_name'),
            'src_condition': request.form.get('src_condition'),
            'check_columns': request.form.get('check_columns')
        }
        # Filter out None values just in case
        config_data = {k: v for k, v in config_data.items() if v is not None}
        save_config(config_data)
    except Exception as e:
        print(f"Warning: Could not save config: {e}")

    return Response(stream_with_context(transfer_worker(request.form)), mimetype='application/x-ndjson')

if __name__ == '__main__':
    app.run(debug=True, port=5000)
