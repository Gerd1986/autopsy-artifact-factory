#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Autopsy Universal Wizard
=======================

- Datenquellen: CSV / SQLite / Text+Regex
- Plugin-Typen: Geo-Track / Geo-Bookmark / Mobile / Bluetooth / Call
- Manuelles Mapping: pro Feld eine Combobox (Spalten einzeln auswählbar)
- Parser lokal (pandas) zum Testen
- Template-Generator für Autopsy 4.22:
  - Autopsy API calls sind KOMMENTIERT
  - findFiles/Tempfile/Parsing/Artefakte als ausführlicher Pseudocode
  - Parser direkt im Plugin eingebettet (ohne pandas, CSV+Regex) => kein Import deiner Wizard-Datei nötig
  - Geo: Embedded Parser liefert timestamp_epoch (int, UTC) + timestamp_str
  - SQLite im Plugin: JDBC-Pseudocodeblock enthalten (weil Jython typischerweise kein sqlite3 hat)
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import pandas as pd
import sqlite3
import re
import datetime
import os
import json
import csv
import shutil
import tempfile
import subprocess
import shlex
import uuid

# ============================================================
# LOKALER NORMALIZER (für Wizard-Parser, Python3 + pandas)
# ============================================================

def normalize_timestamp(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    if s.isdigit():
        try:
            num = int(s)
            if num > 1_000_000_000_000:
                num //= 1000
            return datetime.datetime.utcfromtimestamp(num).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
    ]
    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue
    return s

def normalize_float(val, mn=None, mx=None):
    try:
        f = float(str(val).replace(",", "."))
        if mn is not None and f < mn:
            return None
        if mx is not None and f > mx:
            return None
        return f
    except Exception:
        return None

def normalize_phone(val):
    if not val:
        return None
    s = str(val)
    plus = s.strip().startswith("+")
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    return ("+" + digits) if plus else digits

def normalize_mac(val):
    if not val:
        return None
    s = re.sub(r"[^0-9A-Fa-f]", "", str(val)).upper()
    if len(s) == 12:
        return ":".join(s[i:i+2] for i in range(0, 12, 2))
    return s

def normalize_duration(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
        if len(nums) == 2:
            return nums[0] * 60 + nums[1]
        if len(nums) == 3:
            return nums[0] * 3600 + nums[1] * 60 + nums[2]
    except Exception:
        pass
    return None



# ============================================================
# HELFER
# ============================================================

def unique_temp_name(original_name, prefix="tmp_"):
    stem, ext = os.path.splitext(original_name)
    return prefix + stem + "_" + uuid.uuid4().hex[:12] + ext

def sqlite_sidecar_paths(db_path):
    return [db_path + "-wal", db_path + "-shm"]

def copy_sqlite_with_sidecars(db_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    dst_db = os.path.join(target_dir, unique_temp_name(os.path.basename(db_path), "sqlite_"))
    shutil.copy2(db_path, dst_db)
    for sidecar in sqlite_sidecar_paths(db_path):
        if os.path.exists(sidecar):
            shutil.copy2(sidecar, dst_db + sidecar[len(db_path):])
    return dst_db

def preprocess_steps_from_meta(meta):
    steps = []
    raw_steps = meta.get("preprocess_steps")
    if isinstance(raw_steps, list) and raw_steps:
        for idx, step in enumerate(raw_steps):
            if not isinstance(step, dict):
                continue
            cmd = str(step.get("command") or "").strip()
            if not cmd:
                continue
            steps.append({
                "name": str(step.get("name") or ("step_%d" % (idx + 1))).strip() or ("step_%d" % (idx + 1)),
                "command": cmd,
                "output_suffix": str(step.get("output_suffix") or "").strip() or ".txt",
                "shell": bool(step.get("shell", False)),
            })
    elif meta.get("preprocess_enabled"):
        cmd = str(meta.get("preprocess_command") or "").strip()
        if cmd:
            steps.append({
                "name": "step_1",
                "command": cmd,
                "output_suffix": str(meta.get("preprocess_output_suffix") or "").strip() or ".txt",
                "shell": False,
            })
    return steps

def build_preprocess_output_path(current_input, preferred_suffix=None, step_index=None):
    stem, ext = os.path.splitext(os.path.basename(current_input))
    suffix = preferred_suffix if preferred_suffix is not None else ext
    if not suffix:
        suffix = ".out"
    tmp_dir = tempfile.mkdtemp(prefix="wizard_pre_")
    tag = "" if step_index is None else "_step%d" % (step_index + 1)
    return os.path.join(tmp_dir, stem + tag + "_preprocessed" + suffix)

def run_single_preprocess_step(command_template, input_path, output_path, shell=False, script_dir=None, preview_mode=False):
    cmd_template = command_template
    if preview_mode and not script_dir:
        script_dir = os.path.dirname(os.path.abspath(input_path))
    if script_dir:
        cmd_template = cmd_template.replace("{script_dir}", str(script_dir).replace("\\", "/"))
    command = cmd_template.format(input=input_path, output=output_path)
    if shell:
        pipe = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(shlex.split(command), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_data, stderr_data = pipe.communicate()
    if pipe.returncode != 0:
        if stderr_data is None:
            stderr_text = ""
        else:
            try:
                stderr_text = stderr_data.decode("utf-8", "replace")
            except Exception:
                stderr_text = str(stderr_data)
        raise RuntimeError("Preprocessing fehlgeschlagen: " + stderr_text)
    if not os.path.exists(output_path):
        with open(output_path, "wb") as f:
            if stdout_data:
                f.write(stdout_data)
    return pipe, command

def run_preprocessing_if_needed(meta, preview_mode=False):
    new_meta = dict(meta)
    steps = preprocess_steps_from_meta(meta)
    if not steps:
        return new_meta
    current_input = meta["path"]
    script_dir = meta.get("script_dir")
    logs = []
    for idx, step in enumerate(steps):
        output_path = build_preprocess_output_path(current_input, step.get("output_suffix") or ".txt", idx)
        pipe, command = run_single_preprocess_step(step["command"], current_input, output_path, step.get("shell", False), script_dir, preview_mode)
        logs.append({"index": idx + 1, "name": step.get("name"), "command": command, "input": current_input, "output": output_path, "returncode": pipe.returncode})
        current_input = output_path
    new_meta["preprocess_original_path"] = meta["path"]
    new_meta["path"] = current_input
    new_meta["preprocess_logs"] = logs
    return new_meta

# ============================================================
# LOADER (Wizard – mit pandas)
# ============================================================

def load_csv_preview(path, sep):
    if sep == "\\t":
        sep = "\t"
    return pd.read_csv(path, sep=sep, nrows=50, encoding="utf-8-sig")

def load_sqlite_preview(meta):
    temp_dir = tempfile.mkdtemp(prefix="wizard_sqlite_preview_")
    db_copy = copy_sqlite_with_sidecars(meta["path"], temp_dir)
    con = sqlite3.connect(db_copy)
    try:
        if meta.get("query"):
            df = pd.read_sql_query(meta["query"], con)
        else:
            tabs = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)
            if tabs.empty:
                raise ValueError("Keine Tabellen in SQLite gefunden.")
            table = meta.get("table") or tabs.iloc[0, 0]
            df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 50", con)
        return df
    finally:
        con.close()

def load_regex_preview(path, pattern):
    pat = re.compile(pattern)
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = pat.search(line)
            if m:
                rows.append(m.groupdict())
    return pd.DataFrame(rows)

# ============================================================
# PARSER (Wizard – lokal, pandas)
# ============================================================

def parse_full_local(meta, mapping, plugin_type):
    meta = run_preprocessing_if_needed(meta, preview_mode=True)
    st = meta["source_type"]

    if st == "csv":
        sep = meta["sep"]
        if sep == "\\t":
            sep = "\t"
        df = pd.read_csv(meta["path"], sep=sep, encoding="utf-8-sig")

    elif st == "sqlite":
        temp_dir = tempfile.mkdtemp(prefix="wizard_sqlite_parse_")
        db_copy = copy_sqlite_with_sidecars(meta["path"], temp_dir)
        con = sqlite3.connect(db_copy)
        try:
            if meta.get("query"):
                df = pd.read_sql_query(meta["query"], con)
            else:
                df = pd.read_sql_query(f"SELECT * FROM {meta['table']}", con)
        finally:
            con.close()

    elif st == "regex":
        df = load_regex_preview(meta["path"], meta["regex"])

    else:
        raise ValueError("Unknown source_type")

    parsed = []
    for _, row in df.iterrows():
        rec = {}

        if plugin_type == "Geo-Track":
            # lokal: wir behalten String; epoch wird im embedded Parser für Autopsy erzeugt
            if mapping["Timestamp"]:
                rec["timestamp"] = normalize_timestamp(row[mapping["Timestamp"]])
            if mapping["Longitude"]:
                rec["longitude"] = normalize_float(row[mapping["Longitude"]], -180, 180)
            if mapping["Latitude"]:    
                rec["latitude"]  = normalize_float(row[mapping["Latitude"]],  -90,  90)
            if mapping["Geschwindigkeit"]:    
                rec["speed"]  = normalize_float(row[mapping["Geschwindigkeit"]],  -90,  90)

        elif plugin_type == "Last-Position":
            # lokal: wir behalten String; epoch wird im embedded Parser für Autopsy erzeugt
            if mapping["Kommentar"]:
                rec["remark"] = row[mapping["Kommentar"]]
            if mapping["Timestamp"]:
                rec["timestamp"] = normalize_timestamp(row[mapping["Timestamp"]])
            if mapping["Longitude"]:
                rec["longitude"] = normalize_float(row[mapping["Longitude"]], -180, 180)
            if mapping["Latitude"]:    
                rec["latitude"]  = normalize_float(row[mapping["Latitude"]],  -90,  90)           

        elif plugin_type == "Geo-Bookmark":
            if mapping["Kommentar"]:
                rec["remark"] = row[mapping["Kommentar"]]
            if mapping["Longitude"]:
                rec["longitude"] = normalize_float(row[mapping["Longitude"]], -180, 180)
            if mapping["Latitude"]:    
                rec["latitude"]  = normalize_float(row[mapping["Latitude"]],  -90,  90)   
        
        elif plugin_type == "Mobile":
            if mapping["Nachname"]:
                rec["lastname"]  = row[mapping["Nachname"]]
            if mapping["Vorname"]:
                rec["firstname"] = row[mapping["Vorname"]]
            if mapping["Telefonnummer"]:
                rec["phone"]     = normalize_phone(row[mapping["Telefonnummer"]])
            if mapping["BluetoothAdresse"]:
                rec["bt_mac"]    = normalize_mac(row[mapping["BluetoothAdresse"]])
            
        elif plugin_type == "Bluetooth":
            if mapping["Geraetename"]:
                rec["devicename"]  = row[mapping["Geraetename"]]
            if mapping["BluetoothAdresse"]:
                rec["bt_mac"]    = normalize_mac(row[mapping["BluetoothAdresse"]])    

        elif plugin_type == "Call":
            if mapping["Anrufername"]:
                rec["caller"]           = row[mapping["Anrufername"]]
            if mapping["Timestamp"]:    
                rec["timestamp"] = normalize_timestamp(row[mapping["Timestamp"]])
            if mapping["MACAdresse"]:
                rec["caller_mac"]       = normalize_mac(row[mapping["MACAdresse"]])
            if mapping["Angerufener"]:
                rec["callee"]           = row[mapping["Angerufener"]]
            if mapping["Nummer"]:
                rec["callee_number"]    = normalize_phone(row[mapping["Nummer"]])
            if mapping["Dauer"]:   
                rec["duration_seconds"] = normalize_duration(row[mapping["Dauer"]])

        parsed.append(rec)
    

    return pd.DataFrame(parsed)

# ============================================================
# EMBEDDED PARSER (Template – ohne pandas, Jython-tauglicher für CSV+Regex)
# ============================================================

EMBEDDED_PARSER_BLOCK = r'''
# ===============================
# Embedded Parser (no pandas)
# Supports: CSV, Regex/Text
# Geo: emits timestamp_epoch (int UTC) + timestamp_str
# Extras: preprocessing, sqlite sidecars, duplicate-safe temp names
# ===============================

import csv
import re
import datetime
import os
import shutil
import tempfile
import subprocess
import shlex
import uuid

def _timestamp_to_epoch_and_str(val):
    if val is None:
        return (None, None)
    s = str(val).strip()
    if not s:
        return (None, None)
    if re.match(r"^\d+$", s):
        try:
            num = int(s)
            if num > 1000000000000:
                num //= 1000
            dt = datetime.datetime.utcfromtimestamp(num)
            return (int(num), dt.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            return (None, s)
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d, %H:%M:%S",
        "%Y/%m/%d, %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y",
    ]
    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(s, fmt)
            epoch = int((dt - datetime.datetime(1970, 1, 1)).total_seconds())
            return (epoch, dt.strftime("%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue
    return (None, s)

def _normalize_float(val, mn=None, mx=None):
    if val is None:
        return None
    try:
        f = float(str(val).replace(",", "."))
        if mn is not None and f < mn:
            return None
        if mx is not None and f > mx:
            return None
        return f
    except Exception:
        return None

def _normalize_phone(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    plus = s.startswith("+")
    digits = re.sub(r"\D", "", s)
    if not digits:
        return None
    return ("+" + digits) if plus else digits

def _normalize_mac(val):
    if val is None:
        return None
    s = re.sub(r"[^0-9A-Fa-f]", "", str(val)).upper()
    if len(s) == 12:
        return ":".join([s[i:i+2] for i in range(0, 12, 2)])
    return s

def _normalize_duration(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if re.match(r"^\d+$", s):
        return int(s)
    parts = s.split(":")
    try:
        nums = [int(p) for p in parts]
        if len(nums) == 2:
            return nums[0] * 60 + nums[1]
        if len(nums) == 3:
            return nums[0] * 3600 + nums[1] * 60 + nums[2]
    except Exception:
        pass
    return None

def _unique_temp_name(original_name, prefix="tmp_"):
    stem, ext = os.path.splitext(original_name)
    return prefix + stem + "_" + uuid.uuid4().hex[:12] + ext

def _copy_sqlite_with_sidecars(db_path, dst_dir):
    if not os.path.isdir(dst_dir):
        os.makedirs(dst_dir)
    dst_db = os.path.join(dst_dir, _unique_temp_name(os.path.basename(db_path), "sqlite_"))
    shutil.copy2(db_path, dst_db)
    for suffix in ["-wal", "-shm"]:
        src = db_path + suffix
        if os.path.exists(src):
            shutil.copy2(src, dst_db + suffix)
    return dst_db

def _preprocess_steps_from_meta(meta):
    steps = []
    raw_steps = meta.get("preprocess_steps")
    if raw_steps:
        for idx, step in enumerate(raw_steps):
            if not isinstance(step, dict):
                continue
            cmd = str(step.get("command") or "").strip()
            if not cmd:
                continue
            steps.append({
                "name": str(step.get("name") or ("step_%d" % (idx + 1))).strip(),
                "command": cmd,
                "output_suffix": str(step.get("output_suffix") or "").strip() or ".txt",
                "shell": bool(step.get("shell", False)),
            })
    elif meta.get("preprocess_enabled"):
        cmd = str(meta.get("preprocess_command") or "").strip()
        if cmd:
            steps.append({
                "name": "step_1",
                "command": cmd,
                "output_suffix": str(meta.get("preprocess_output_suffix") or "").strip() or ".txt",
                "shell": False,
            })
    return steps

def _run_preprocess_step(step, input_path, step_index, script_dir):
    out_suffix = step.get("output_suffix") or os.path.splitext(input_path)[1] or ".txt"
    tmp_dir = tempfile.mkdtemp(prefix="autopsy_pre_")
    out_path = os.path.join(tmp_dir, "preprocessed_step_%d" % (step_index + 1) + out_suffix)
    cmd_template = step["command"]
    if script_dir:
        cmd_template = cmd_template.replace("{script_dir}", str(script_dir).replace("\\", "/"))
    cmd = cmd_template.format(input=input_path, output=out_path)
    if step.get("shell", False):
        pipe = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        pipe = subprocess.Popen(shlex.split(cmd), shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout_data, stderr_data = pipe.communicate()
    if pipe.returncode != 0:
        if stderr_data is None:
            stderr_text = ""
        else:
            try:
                stderr_text = stderr_data.decode("utf-8", "replace")
            except Exception:
                stderr_text = str(stderr_data)
        raise Exception("Preprocessing fehlgeschlagen: " + stderr_text)
    if not os.path.exists(out_path):
        f = open(out_path, "wb")
        try:
            if stdout_data:
                f.write(stdout_data)
        finally:
            f.close()
    return out_path

def _run_preprocess_if_needed(meta):
    steps = _preprocess_steps_from_meta(meta)
    if not steps:
        return dict(meta)
    new_meta = dict(meta)
    current_path = meta["path"]
    script_dir = meta.get("script_dir")
    for idx, step in enumerate(steps):
        current_path = _run_preprocess_step(step, current_path, idx, script_dir)
    new_meta["path"] = current_path
    return new_meta

def _iter_rows_from_csv_file(path, sep):
    if sep == "\\t":
        sep = "\t"
    with open(path, "r") as f:
        reader = csv.DictReader(f, delimiter=sep)
        for row in reader:
            yield row

def _iter_rows_from_regex_file(path, pattern):
    pat = re.compile(pattern)
    with open(path, "r") as f:
        for line in f:
            m = pat.search(line)
            if m:
                yield m.groupdict()

def parse_rows(meta, mapping, plugin_type):
    st = meta.get("source_type")
    meta = _run_preprocess_if_needed(meta)
    if st == "csv":
        src_iter = _iter_rows_from_csv_file(meta["path"], meta.get("sep", ","))
    elif st == "regex":
        src_iter = _iter_rows_from_regex_file(meta["path"], meta["regex"])
    elif st == "sqlite":
        sql = meta.get("query") or ("SELECT * FROM " + meta.get("table"))
        src_iter = parse_rows_sqlite_jdbc(meta["path"], sql)
    else:
        raise Exception("Unknown source_type: %s" % str(st))
    out = []
    for row in src_iter:
        rec = {}
        if plugin_type == "Geo-Track":
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping.get("Timestamp")))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"] = ts_str
            rec["longitude"] = _normalize_float(row.get(mapping.get("Longitude")), -180, 180)
            rec["latitude"] = _normalize_float(row.get(mapping.get("Latitude")), -90, 90)
            rec["speed"] = _normalize_float(row.get(mapping.get("Geschwindigkeit")))
        elif plugin_type == "Last-Position":
            rec["remark"] = row.get(mapping.get("Kommentar"))
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping.get("Timestamp")))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"] = ts_str
            rec["longitude"] = _normalize_float(row.get(mapping.get("Longitude")), -180, 180)
            rec["latitude"] = _normalize_float(row.get(mapping.get("Latitude")), -90, 90)
        elif plugin_type == "Geo-Bookmark":
            rec["remark"] = row.get(mapping.get("Kommentar"))
            rec["longitude"] = _normalize_float(row.get(mapping.get("Longitude")), -180, 180)
            rec["latitude"] = _normalize_float(row.get(mapping.get("Latitude")), -90, 90)
        elif plugin_type == "Mobile":
            rec["lastname"] = row.get(mapping.get("Nachname"))
            rec["firstname"] = row.get(mapping.get("Vorname"))
            rec["phone"] = _normalize_phone(row.get(mapping.get("Telefonnummer")))
            rec["bt_mac"] = _normalize_mac(row.get(mapping.get("BluetoothAdresse")))
        elif plugin_type == "Bluetooth":
            rec["devicename"] = row.get(mapping.get("Geraetename"))
            rec["bt_mac"] = _normalize_mac(row.get(mapping.get("BluetoothAdresse")))
        elif plugin_type == "Call":
            rec["caller"] = row.get(mapping.get("Anrufername"))
            rec["caller_mac"] = _normalize_mac(row.get(mapping.get("MACAdresse")))
            rec["callee"] = row.get(mapping.get("Angerufener"))
            rec["callee_number"] = _normalize_phone(row.get(mapping.get("Nummer")))
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping.get("Timestamp")))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"] = ts_str
            rec["duration_seconds"] = _normalize_duration(row.get(mapping.get("Dauer")))
        out.append(rec)
    return out
'''

SQLITE_JDBC_BLOCK = r'''
from java.sql import DriverManager

def parse_rows_sqlite_jdbc(db_path, sql_query):
    rows = []
    tmp_dir = tempfile.mkdtemp(prefix="autopsy_sqlite_")
    db_copy = _copy_sqlite_with_sidecars(db_path, tmp_dir)
    conn = DriverManager.getConnection("jdbc:sqlite:" + db_copy)
    stmt = conn.createStatement()
    rs = stmt.executeQuery(sql_query)
    meta = rs.getMetaData()
    col_count = meta.getColumnCount()
    while rs.next():
        rec = {}
        for i in range(1, col_count + 1):
            name = meta.getColumnName(i)
            rec[name] = rs.getString(i)
        rows.append(rec)
    rs.close()
    stmt.close()
    conn.close()
    return rows
'''

# ============================================================
# TEMPLATE GENERATOR (Autopsy – kommentiert, Parser eingebettet)
# ============================================================


def build_artifact_block(plugin_type):
    if plugin_type == "Geo-Track":
        return r'''
                    timestamp_epoch = row.get("timestamp_epoch")
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")
                    speed = row.get("speed")
                    attrs = ArrayList()
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
                    if speed is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_VELOCITY, moduleName, float(speed)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_TRACKPOINT), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Last-Position":
        return r'''
                    remark = row.get("remark")
                    timestamp_epoch = row.get("timestamp_epoch")
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")
                    attrs = ArrayList()
                    if remark is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, str(remark)))
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_LAST_KNOWN_LOCATION), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Geo-Bookmark":
        return r'''
                    remark = row.get("remark")
                    latitude = row.get("latitude")
                    longitude = row.get("longitude")
                    attrs = ArrayList()
                    if remark is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, str(remark)))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_BOOKMARK), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Mobile":
        return r'''
                    firstname = row.get("firstname")
                    lastname = row.get("lastname")
                    phone = row.get("phone")
                    bt_mac = row.get("bt_mac")
                    attrs = ArrayList()
                    if lastname is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME, moduleName, str(lastname)))
                    if firstname is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, str(firstname)))
                    if phone is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER, moduleName, str(phone)))
                    if bt_mac is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, str(bt_mac)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_CONTACT), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Bluetooth":
        return r'''
                    devicename = row.get("devicename")
                    bt_mac = row.get("bt_mac")
                    attrs = ArrayList()
                    if devicename is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DEVICE_NAME, moduleName, str(devicename)))
                    if bt_mac is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, str(bt_mac)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_BLUETOOTH_PAIRING), attrs)
                    blackboard.indexArtifact(art)
'''
    else:
        return r'''
                    caller = row.get("caller")
                    caller_mac = row.get("caller_mac")
                    callee = row.get("callee")
                    callee_number = row.get("callee_number")
                    timestamp_epoch = row.get("timestamp_epoch")
                    duration_seconds = row.get("duration_seconds")
                    attrs = ArrayList()
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if caller is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, str(caller)))
                    if caller_mac is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, str(caller_mac)))
                    if callee is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, str(callee)))
                    if callee_number is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER, moduleName, str(callee_number)))
                    if duration_seconds is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, "Duration (s): %s" % str(duration_seconds)))
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_CALLLOG), attrs)
                    blackboard.indexArtifact(art)
'''

def build_autopsy_template(plugin_type, filename, meta, mapping, embed_parser=True):
    meta_literal = repr(meta)
    mapping_literal = repr(mapping)

    header = f'''# coding=utf-8

FILENAME = {filename!r}
META = {meta_literal}
MAPPING = {mapping_literal}

from org.sleuthkit.autopsy.ingest import IngestModuleFactoryAdapter, DataSourceIngestModule, IngestModule
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.datamodel import Blackboard, BlackboardAttribute, BlackboardArtifact
from org.sleuthkit.autopsy.datamodel import ContentUtils
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import IngestMessage
from java.util import ArrayList
from java.io import File
import os
'''

    parts = [header]
    if embed_parser:
        parts.append(EMBEDDED_PARSER_BLOCK)
        parts.append(SQLITE_JDBC_BLOCK)

    artifact_block = build_artifact_block(plugin_type)

    core = f'''
class UniversalPluginFactory(IngestModuleFactoryAdapter):
    def getModuleDisplayName(self):
        return "{plugin_type} Universal Plugin"
    def getModuleDescription(self):
        return "Generated by stable wizard + preprocessing"
    def getModuleVersionNumber(self):
        return "2.0"
    def isDataSourceIngestModuleFactory(self):
        return True
    def createDataSourceIngestModule(self, options):
        return UniversalPluginModule()

class UniversalPluginModule(DataSourceIngestModule):
    def startUp(self, context):
        self.context = context
        self.moduleName = "{plugin_type} Universal Plugin"

    def process(self, dataSource, progressBar):
        case = Case.getCurrentCase()
        fm = case.getServices().getFileManager()
        blackboard = case.getServices().getBlackboard()

        files = fm.findFiles(dataSource, FILENAME)
        if files is None or files.isEmpty():
            return IngestModule.ProcessResult.OK

        for file in files:
            try:
                tempDir = os.path.join(Case.getCurrentCase().getTempDirectory(), "{plugin_type}_Universal")
                try:
                    os.mkdir(tempDir)
                except:
                    pass

                outFile = os.path.join(tempDir, _unique_temp_name(file.getName(), "evidence_"))
                ContentUtils.writeToFile(file, File(outFile))

                meta = dict(META)
                meta["path"] = outFile
                meta["script_dir"] = os.path.dirname(__file__).replace("\\\\", "/")

                if meta.get("source_type") == "sqlite":
                    for suffix in ["-wal", "-shm"]:
                        try:
                            related = fm.findFiles(dataSource, file.getName() + suffix)
                            if related is not None and not related.isEmpty():
                                ContentUtils.writeToFile(related.get(0), File(outFile + suffix))
                        except:
                            pass

                rows = parse_rows(meta, MAPPING, "{plugin_type}")
                moduleName = self.moduleName

                for row in rows:
{artifact_block}
            except Exception as e:
                IngestServices.getInstance().postMessage(
                    IngestMessage.createErrorMessage(self.moduleName, "Error processing " + file.getName(), str(e))
                )
                pass

        return IngestModule.ProcessResult.OK
'''
    parts.append(core)
    return "\n".join(parts)

# ============================================================
# GUI (Wizard)
# ============================================================

class Wizard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Autopsy Universal Wizard (stable + preprocessing + WAL/SHM + duplicate-safe)")
        self.geometry("1210x900")

        self.meta = {}
        self.preview = None
        self.mapping_widgets = {}

        self.plugin_type = tk.StringVar(value="Geo-Track")
        self.csv_sep = tk.StringVar(value=",")
        self.embed_parser = tk.BooleanVar(value=True)
        self.preprocess_enabled = tk.BooleanVar(value=False)
        self.preprocess_command = tk.StringVar(value='perl "{script_dir}/getNMEA.pl" "{input}"')
        self.preprocess_output_suffix = tk.StringVar(value=".txt")
        self.preprocess_steps = []

        self._build_ui()

    def _build_ui(self):
        top = tk.Frame(self)
        top.pack(fill="x", pady=6, padx=8)

        ttk.Label(top, text="Plugin-Typ:").pack(side="left")
        ttk.Combobox(
            top, textvariable=self.plugin_type,
            values=["Geo-Track", "Last-Position", "Geo-Bookmark", "Mobile", "Bluetooth", "Call"],
            state="readonly", width=10
        ).pack(side="left", padx=6)

        ttk.Button(top, text="CSV wählen", command=self.choose_csv).pack(side="left", padx=6)
        ttk.Button(top, text="SQLite wählen", command=self.choose_sqlite).pack(side="left", padx=6)
        ttk.Button(top, text="Text/Regex wählen", command=self.choose_regex).pack(side="left", padx=6)

        ttk.Label(top, text="CSV-Trennzeichen:").pack(side="left", padx=(10, 2))
        ttk.Combobox(
            top, textvariable=self.csv_sep,
            values=[",", ";", "\\t", "|"],
            state="readonly", width=6
        ).pack(side="left", padx=6)

        ttk.Checkbutton(
            top,
            text="Parser ins Plugin einbetten (ohne pandas)",
            variable=self.embed_parser
        ).pack(side="left", padx=10)

        ttk.Button(top, text="Vorschau laden", command=self.load_preview).pack(side="left", padx=6)


        opts = tk.LabelFrame(self, text="Preprocessing")
        opts.pack(fill="x", padx=10, pady=6)

        row1 = tk.Frame(opts)
        row1.pack(fill="x", padx=8, pady=5)
        ttk.Checkbutton(row1, text="Preprocessing aktiv", variable=self.preprocess_enabled).pack(side="left")
        ttk.Label(row1, text="Legacy Step 1:").pack(side="left", padx=(12, 4))
        ttk.Entry(row1, textvariable=self.preprocess_command, width=72).pack(side="left", fill="x", expand=True)
        ttk.Label(row1, text="Suffix:").pack(side="left", padx=(10, 4))
        ttk.Entry(row1, textvariable=self.preprocess_output_suffix, width=10).pack(side="left", padx=(0, 6))

        row2 = tk.Frame(opts)
        row2.pack(fill="x", padx=8, pady=5)
        ttk.Button(row2, text="Preprocess-Pipeline bearbeiten", command=self.edit_preprocess_pipeline).pack(side="left")
        ttk.Label(row2, text="Mehrere Steps mit {input}/{output}; script_dir wird im Plugin automatisch gesetzt.").pack(side="left", padx=12)

        ttk.Label(self, text="Vorschau (erste ~50 Zeilen):").pack(anchor="w", padx=10)
        self.preview_box = scrolledtext.ScrolledText(self, height=18)
        self.preview_box.pack(fill="both", padx=10, pady=6)

        self.mapping_frame = tk.LabelFrame(self, text="Spalten-Mapping (jede Spalte einzeln auswählen)")
        self.mapping_frame.pack(fill="x", padx=10, pady=8)

        bottom = tk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=10)

        ttk.Button(bottom, text="Parser testen (lokal)", command=self.test_parser).pack(side="left", padx=6)
        ttk.Button(bottom, text="Normalisierte CSV speichern", command=self.export_parsed_csv).pack(side="left", padx=6)
        ttk.Button(bottom, text="Autopsy-Template speichern", command=self.save_template).pack(side="left", padx=6)

    # ---------------------------
    # Source selection
    # ---------------------------


    def _apply_common_meta_options(self):
        if not self.meta:
            return
        self.meta["preprocess_enabled"] = bool(self.preprocess_enabled.get())
        self.meta["preprocess_command"] = self.preprocess_command.get().strip()
        self.meta["preprocess_output_suffix"] = self.preprocess_output_suffix.get().strip() or ".txt"
        self.meta["preprocess_steps"] = list(self.preprocess_steps)
        if self.meta.get("source_type") == "csv":
            self.meta["sep"] = self.csv_sep.get()

    def edit_preprocess_pipeline(self):
        win = tk.Toplevel(self)
        win.title("Preprocess-Pipeline")
        win.geometry("1100x520")

        ttk.Label(win, text="Jeder Step nutzt {input} und {output}. Reihenfolge = Ausführungsreihenfolge.").pack(anchor="w", padx=10, pady=6)
        listbox = tk.Listbox(win, height=10)
        listbox.pack(fill="x", padx=10, pady=6)

        def refresh():
            listbox.delete(0, "end")
            for idx, step in enumerate(self.preprocess_steps):
                shell_txt = "shell" if step.get("shell", False) else "noshell"
                listbox.insert("end", "%d. %s [%s] -> %s :: %s" % (idx + 1, step.get("name", "step"), shell_txt, step.get("output_suffix", ""), step.get("command", "")))
            self.preprocess_enabled.set(bool(self.preprocess_steps or self.preprocess_command.get().strip()))

        def edit_step(existing=None):
            dlg = tk.Toplevel(win)
            dlg.title("Preprocess-Step")
            dlg.geometry("950x340")

            name_var = tk.StringVar(value=(existing or {}).get("name", "step_%d" % (len(self.preprocess_steps) + 1)))
            suffix_var = tk.StringVar(value=(existing or {}).get("output_suffix", ".txt"))
            shell_var = tk.BooleanVar(value=(existing or {}).get("shell", False))

            ttk.Label(dlg, text="Name:").pack(anchor="w", padx=10, pady=(10, 2))
            ttk.Entry(dlg, textvariable=name_var, width=40).pack(anchor="w", padx=10)

            row = tk.Frame(dlg)
            row.pack(fill="x", padx=10, pady=8)
            ttk.Label(row, text="Output-Suffix:").pack(side="left")
            ttk.Entry(row, textvariable=suffix_var, width=12).pack(side="left", padx=6)
            ttk.Checkbutton(row, text="shell=True", variable=shell_var).pack(side="left", padx=16)

            ttk.Label(dlg, text="Command:").pack(anchor="w", padx=10, pady=(8, 2))
            cmd_box = scrolledtext.ScrolledText(dlg, height=10, wrap="word")
            cmd_box.pack(fill="both", expand=True, padx=10, pady=4)
            cmd_box.insert("1.0", (existing or {}).get("command", ""))

            result = []
            def ok():
                result.append({
                    "name": name_var.get().strip() or ("step_%d" % (len(self.preprocess_steps) + 1)),
                    "command": cmd_box.get("1.0", "end-1c").strip(),
                    "output_suffix": suffix_var.get().strip() or ".txt",
                    "shell": bool(shell_var.get()),
                })
                dlg.destroy()

            ttk.Button(dlg, text="OK", command=ok).pack(pady=8)
            dlg.grab_set()
            dlg.wait_window()
            return result[0] if result else None

        btns = tk.Frame(win)
        btns.pack(fill="x", padx=10, pady=4)

        def add_step():
            step = edit_step()
            if step and step.get("command"):
                self.preprocess_steps.append(step)
                refresh()

        def edit_selected():
            sel = listbox.curselection()
            if not sel:
                return
            idx = sel[0]
            step = edit_step(self.preprocess_steps[idx])
            if step and step.get("command"):
                self.preprocess_steps[idx] = step
                refresh()
                listbox.selection_set(idx)

        def delete_selected():
            sel = listbox.curselection()
            if not sel:
                return
            del self.preprocess_steps[sel[0]]
            refresh()

        def move_up():
            sel = listbox.curselection()
            if not sel or sel[0] == 0:
                return
            idx = sel[0]
            self.preprocess_steps[idx - 1], self.preprocess_steps[idx] = self.preprocess_steps[idx], self.preprocess_steps[idx - 1]
            refresh()
            listbox.selection_set(idx - 1)

        def move_down():
            sel = listbox.curselection()
            if not sel or sel[0] >= len(self.preprocess_steps) - 1:
                return
            idx = sel[0]
            self.preprocess_steps[idx + 1], self.preprocess_steps[idx] = self.preprocess_steps[idx], self.preprocess_steps[idx + 1]
            refresh()
            listbox.selection_set(idx + 1)

        ttk.Button(btns, text="Hinzufügen", command=add_step).pack(side="left", padx=4)
        ttk.Button(btns, text="Bearbeiten", command=edit_selected).pack(side="left", padx=4)
        ttk.Button(btns, text="Löschen", command=delete_selected).pack(side="left", padx=4)
        ttk.Button(btns, text="Hoch", command=move_up).pack(side="left", padx=4)
        ttk.Button(btns, text="Runter", command=move_down).pack(side="left", padx=4)

        refresh()

    def choose_csv(self):
        path = filedialog.askopenfilename(
            title="CSV auswählen",
            filetypes=[("CSV/TSV/Text", "*.csv;*.tsv;*.txt"), ("Alle Dateien", "*.*")]
        )
        if not path:
            return
        self.meta = {"source_type": "csv", "path": path, "sep": self.csv_sep.get()}
        self._apply_common_meta_options()
        messagebox.showinfo("CSV gewählt", path)

    def choose_sqlite(self):
        path = filedialog.askopenfilename(
            title="SQLite auswählen",
            filetypes=[("SQLite", "*.db;*.sqlite"), ("Alle Dateien", "*.*")]
        )
        if not path:
            return

        meta = {"source_type": "sqlite", "path": path, "query": None, "table": ""}

        if messagebox.askyesno("SQL", "Eigene SQL-Abfrage eingeben (statt Tabellenwahl)?"):
            q = self._ask_text("SQL eingeben", "SELECT * FROM table LIMIT 50;")
            if not q:
                return
            meta["query"] = q
        else:
            con = sqlite3.connect(path)
            tabs = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)
            con.close()
            if tabs.empty:
                messagebox.showerror("Fehler", "Keine Tabellen in SQLite gefunden.")
                return
            meta["table"] = tabs.iloc[0, 0]

        self.meta = meta
        self._apply_common_meta_options()
        messagebox.showinfo("SQLite gewählt", path)

    def choose_regex(self):
        path = filedialog.askopenfilename(
            title="Textdatei auswählen",
            filetypes=[("Text/Log", "*.txt;*.log"), ("Alle Dateien", "*.*")]
        )
        if not path:
            return
        regex = self._ask_text("Regex (mit Named Groups)", r"(?P<Timestamp>...)(?P<Longitude>...)(?P<Latitude>...)")
        if not regex:
            return
        self.meta = {"source_type": "regex", "path": path, "regex": regex}
        self._apply_common_meta_options()
        messagebox.showinfo("Regex-Datei gewählt", path)

    # ---------------------------
    # Preview
    # ---------------------------

    def load_preview(self):
        if not self.meta:
            messagebox.showerror("Fehler", "Keine Quelle ausgewählt.")
            return

        self._apply_common_meta_options()
        st = self.meta["source_type"]
        try:
            working_meta = run_preprocessing_if_needed(self.meta, preview_mode=True)
            if st == "csv":
                df = load_csv_preview(working_meta["path"], working_meta["sep"])
            elif st == "sqlite":
                df = load_sqlite_preview(working_meta)
            elif st == "regex":
                df = load_regex_preview(working_meta["path"], working_meta["regex"])
            else:
                raise ValueError("Unknown source_type")
        except Exception as e:
            messagebox.showerror("Fehler beim Laden", str(e))
            return

        self.preview = df
        self.preview_box.config(state="normal")
        self.preview_box.delete("1.0", "end")
        self.preview_box.insert("end", df.to_string(index=False))
        self.preview_box.config(state="disabled")

        self._build_mapping_ui(df)

    # ---------------------------
    # Mapping UI
    # ---------------------------

    def _required_fields(self):
        pt = self.plugin_type.get()
        if pt == "Geo-Track":
            return ["Timestamp", "Longitude", "Latitude", "Geschwindigkeit"]
        if pt == "Last-Position":
            return ["Kommentar", "Timestamp", "Longitude", "Latitude"]
        if pt == "Geo-Bookmark":
            return ["Kommentar", "Longitude", "Latitude"]           
        if pt == "Mobile":
            return ["Nachname", "Vorname", "Telefonnummer", "BluetoothAdresse"]
        if pt == "Bluetooth":
            return ["Geraetename", "BluetoothAdresse"]
        return ["Timestamp", "Anrufername", "MACAdresse", "Angerufener", "Nummer", "Dauer"]

    def _build_mapping_ui(self, df):
        for w in self.mapping_frame.winfo_children():
            w.destroy()
        self.mapping_widgets = {}
        cols = list(df.columns)
       
        required = self._required_fields()

        for r, logical in enumerate(required):
            ttk.Label(self.mapping_frame, text=f"{logical}:").grid(row=r, column=0, sticky="e", padx=6, pady=3)
            cb = ttk.Combobox(self.mapping_frame, values=["(keine)"] + cols, state="readonly", width=45)
            cb.grid(row=r, column=1, sticky="w", padx=6, pady=3)
            self.mapping_widgets[logical] = cb

    def get_mapping(self):
        if not self.mapping_widgets:
            raise ValueError("Kein Mapping verfügbar. Erst Vorschau laden.")
        mapping = {}
        for logical, cb in self.mapping_widgets.items():
            col = cb.get()
            if col== "(keine)" or not col:
                mapping[logical] = ""
            else:
                mapping[logical] = col
        return mapping

    # ---------------------------
    # Local test / export
    # ---------------------------

    def test_parser(self):
        if self.preview is None:
            messagebox.showerror("Fehler", "Keine Vorschau geladen.")
            return
        try:
            self._apply_common_meta_options()
            mapping = self.get_mapping()
            self._apply_common_meta_options()
            df = parse_full_local(self.meta, mapping, self.plugin_type.get())
            messagebox.showinfo("Parser OK", f"{len(df)} Zeilen normalisiert.")
        except Exception as e:
            messagebox.showerror("Parser-Fehler", str(e))

    def export_parsed_csv(self):
        if self.preview is None:
            messagebox.showerror("Fehler", "Keine Vorschau geladen.")
            return
        try:
            mapping = self.get_mapping()
            self._apply_common_meta_options()
            df = parse_full_local(self.meta, mapping, self.plugin_type.get())
        except Exception as e:
            messagebox.showerror("Fehler", str(e))
            return

        out = filedialog.asksaveasfilename(
            title="Normalisierte CSV speichern",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")]
        )
        if not out:
            return
        try:
            df.to_csv(out, index=False, encoding="utf-8")
        except Exception as e:
            messagebox.showerror("Fehler beim Speichern", str(e))
            return
        messagebox.showinfo("Gespeichert", out)

    # ---------------------------
    # Template generation
    # ---------------------------

    def save_template(self):
        if self.preview is None:
            messagebox.showerror("Fehler", "Keine Vorschau geladen.")
            return
        try:
            self._apply_common_meta_options()
            mapping = self.get_mapping()
        except Exception as e:
            messagebox.showerror("Mapping-Fehler", str(e))
            return

        filename = os.path.basename(self.meta["path"])
        code = build_autopsy_template(
            plugin_type=self.plugin_type.get(),
            filename=filename,
            meta=self.meta,
            mapping=mapping,
            embed_parser=self.embed_parser.get()
        )

        out = filedialog.asksaveasfilename(
            title="Autopsy Plugin Template speichern",
            defaultextension=".py",
            filetypes=[("Python", "*.py")]
        )
        if not out:
            return

        try:
            with open(out, "w", encoding="utf-8") as f:
                f.write(code)
        except Exception as e:
            messagebox.showerror("Fehler beim Speichern", str(e))
            return

        messagebox.showinfo("Template gespeichert", out)

    # ---------------------------
    # Utility dialog
    # ---------------------------


    def show_meta_json(self):
        if not self.meta:
            messagebox.showerror("Fehler", "Keine Meta vorhanden.")
            return
        self._apply_common_meta_options()
        win = tk.Toplevel(self)
        win.title("Aktuelle Meta")
        box = scrolledtext.ScrolledText(win, width=120, height=40)
        box.pack(fill="both", expand=True)
        box.insert("1.0", json.dumps(self.meta, indent=2, ensure_ascii=False))

    def _ask_text(self, title, default=""):
        win = tk.Toplevel(self)
        win.title(title)
        ttk.Label(win, text=title).pack(pady=6)
        ent = ttk.Entry(win, width=80)
        ent.insert(0, default)
        ent.pack(padx=10, pady=6)
        out = []

        def ok():
            out.append(ent.get())
            win.destroy()

        ttk.Button(win, text="OK", command=ok).pack(pady=6)
        win.grab_set()
        win.wait_window()
        return out[0] if out else None


if __name__ == "__main__":
    Wizard().mainloop()
