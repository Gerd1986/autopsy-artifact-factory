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
# LOADER (Wizard – mit pandas)
# ============================================================

def load_csv_preview(path, sep):
    if sep == "\\t":
        sep = "\t"
    return pd.read_csv(path, sep=sep, nrows=50, encoding="utf-8-sig")

def load_sqlite_preview(path, query=None):
    con = sqlite3.connect(path)
    if query:
        df = pd.read_sql_query(query, con)
    else:
        tabs = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table'", con)
        if tabs.empty:
            con.close()
            raise ValueError("Keine Tabellen in SQLite gefunden.")
        df = pd.read_sql_query(f"SELECT * FROM {tabs.iloc[0,0]} LIMIT 50", con)
    con.close()
    return df

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
    st = meta["source_type"]

    if st == "csv":
        sep = meta["sep"]
        if sep == "\\t":
            sep = "\t"
        df = pd.read_csv(meta["path"], sep=sep, encoding="utf-8-sig")

    elif st == "sqlite":
        con = sqlite3.connect(meta["path"])
        if meta.get("query"):
            df = pd.read_sql_query(meta["query"], con)
        else:
            df = pd.read_sql_query(f"SELECT * FROM {meta['table']}", con)
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
            if mapping["Speed"]:    
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
# ===============================

import csv
import re
import datetime
import os

def _timestamp_to_epoch_and_str(val):
    """
    Returns (epoch_seconds:int|None, timestamp_str:str|None)
    Tries:
    - epoch seconds / epoch ms
    - common datetime formats
    If parsing fails, returns (None, original_string)
    """
    if val is None:
        return (None, None)

    s = str(val).strip()
    if not s:
        return (None, None)

    # epoch seconds/ms
    if re.match(r"^\d+$", s):
        try:
            num = int(s)
            if num > 1000000000000:  # ms
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
            # interpret as UTC; adjust here if your timestamps are local time
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

def _iter_rows_from_csv_file(path, sep):
    if sep == "\\t":
        sep = "\t"
    # Encoding handling in Jython can vary; adjust if needed.
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
    """
    Returns: list of dict rows, normalized per plugin_type
    meta: {source_type: csv|regex|sqlite, path, sep, regex, query, table}
    mapping: logical_field -> column_name (or regex group key)
    """
    st = meta.get("source_type")

    if st == "csv":
        src_iter = _iter_rows_from_csv_file(meta["path"], meta.get("sep", ","))
    elif st == "regex":
        src_iter = _iter_rows_from_regex_file(meta["path"], meta["regex"])
    elif st == "sqlite":
        src_iter = parse_rows_sqlite_jdbc(meta["path"], meta["query"])
    else:
        raise Exception("Unknown source_type: %s" % str(st))

    out = []

    for row in src_iter:
        rec = {}

        if plugin_type == "Geo-Track":
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping["Timestamp"]))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"]   = ts_str
            rec["longitude"] = _normalize_float(row.get(mapping["Longitude"]), -180, 180)
            rec["latitude"]  = _normalize_float(row.get(mapping["Latitude"]),  -90,  90)
            rec["speed"]  = _normalize_float(row.get(mapping["Geschwindigkeit"]),  -90,  90)

        elif plugin_type == "Last-Position":
            rec["remark"]   = row.get(mapping["Kommentar"])
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping["Timestamp"]))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"]   = ts_str          
            rec["longitude"] = _normalize_float(row.get(mapping["Longitude"]), -180, 180)
            rec["latitude"]  = _normalize_float(row.get(mapping["Latitude"]),  -90,  90)

        elif plugin_type == "Geo-Bookmark":
            rec["remark"]   = row.get(mapping["Kommentar"])
            rec["longitude"] = _normalize_float(row.get(mapping["Longitude"]), -180, 180)
            rec["latitude"]  = _normalize_float(row.get(mapping["Latitude"]),  -90,  90)

        elif plugin_type == "Mobile":
            rec["lastname"]  = row.get(mapping["Nachname"])
            rec["firstname"] = row.get(mapping["Vorname"])
            rec["phone"]     = _normalize_phone(row.get(mapping["Telefonnummer"]))
            rec["bt_mac"]    = _normalize_mac(row.get(mapping["BluetoothAdresse"]))
            
        elif plugin_type == "Bluetooth":
            rec["devicename"]  = row.get(mapping["Geraetename"])
            rec["bt_mac"]    = _normalize_mac(row.get(mapping["BluetoothAdresse"]))   

        elif plugin_type == "Call":
            rec["caller"]           = row.get(mapping["Anrufername"])
            rec["caller_mac"]       = _normalize_mac(row.get(mapping["MACAdresse"]))
            rec["callee"]           = row.get(mapping["Angerufener"])
            rec["callee_number"]    = _normalize_phone(row.get(mapping["Nummer"]))
            epoch, ts_str = _timestamp_to_epoch_and_str(row.get(mapping["Timestamp"]))
            rec["timestamp_epoch"] = epoch
            rec["timestamp_str"]   = ts_str
            rec["duration_seconds"] = _normalize_duration(row.get(mapping["Dauer"]))

        out.append(rec)

    return out
'''

SQLITE_JDBC_BLOCK = r'''
# ===============================
# SQLite via JDBC (PSEUDOCODE)
# ===============================
# In Jython usually no 'sqlite3'. Use JDBC:
#
from java.sql import DriverManager
#
def parse_rows_sqlite_jdbc(db_path, sql_query):
    rows = []
    # Ensure SQLite JDBC driver is available to Autopsy/JVM classpath.
    conn = DriverManager.getConnection("jdbc:sqlite:" + db_path)
    stmt = conn.createStatement()
    rs = stmt.executeQuery(sql_query)
    meta = rs.getMetaData()
    col_count = meta.getColumnCount()
    while rs.next():
        rec = {}
        for i in range(1, col_count+1):
            name = meta.getColumnName(i)
            rec[name] = rs.getString(i)
        rows.append(rec)
    rs.close(); stmt.close(); conn.close()
    return rows
    pass
'''

# ============================================================
# TEMPLATE GENERATOR (Autopsy – kommentiert, Parser eingebettet)
# ============================================================

def build_autopsy_template(plugin_type, filename, meta, mapping, embed_parser=True):
    mj = json.dumps(mapping, indent=2, ensure_ascii=False)
    meta_json = json.dumps(meta, indent=2, ensure_ascii=False)

    header = f'''# coding=utf-8

FILENAME = "{filename}"
META = {meta_json}
MAPPING = {mj}

# --- AUTOPSY IMPORTS (COMMENTED) ---
from org.sleuthkit.autopsy.ingest import IngestModuleFactoryAdapter, DataSourceIngestModule, IngestModule
from org.sleuthkit.autopsy.casemodule import Case
from org.sleuthkit.datamodel import Blackboard, BlackboardAttribute, BlackboardArtifact
from org.sleuthkit.autopsy.datamodel import ContentUtils
#
from org.sleuthkit.autopsy.ingest import IngestServices
from org.sleuthkit.autopsy.ingest import IngestMessage
from java.util import ArrayList
from java.io import File, FileOutputStream
'''

    parts = [header]
    if embed_parser:
        parts.append(EMBEDDED_PARSER_BLOCK)
        parts.append(SQLITE_JDBC_BLOCK)

    if plugin_type == "Geo-Track":
        artifact_block = r'''
#                     # ---- GEO / TSK_GPS_TRACKPOINT ----
                    timestamp_epoch = row.get("timestamp_epoch")   #int UTC, recommended for TSK_DATETIME
                    timestamp_str   = row.get("timestamp_str")     #optional string for debugging
                    latitude        = row.get("latitude")
                    longitude       = row.get("longitude")
                    speed           = row.get("speed")
#
                    attrs = ArrayList()
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
#
                    if speed is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_VELOCITY, moduleName, float(speed)))
                    #Optional debug/comment:
                    #if timestamp_str:
                    #    attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, "TS: " + str(timestamp_str)))
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_TRACKPOINT), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Last-Position":
        artifact_block = r'''
#                     # ---- GEO / TSK_GPS_LAST_KNOWN_LOCATION ----
                    remark          = row.get("remark") 
                    timestamp_epoch = row.get("timestamp_epoch")   #int UTC, recommended for TSK_DATETIME
                    timestamp_str   = row.get("timestamp_str")     #optional string for debugging
                    latitude        = row.get("latitude")
                    longitude       = row.get("longitude")
#
                    attrs = ArrayList()
                    if remark is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, remark))                   
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
#
                    #Optional debug/comment:
                    #if timestamp_str:
                    #    attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, "TS: " + str(timestamp_str)))
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_LAST_KNOWN_LOCATION), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Geo-Bookmark":
        artifact_block = r'''
#                     # ---- GEO / TSK_GPS_Bookmark ----
                    remark          = row.get("remark")     
                    latitude        = row.get("latitude")
                    longitude       = row.get("longitude")
#
                    attrs = ArrayList()
                    if remark is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, remark))
                    if latitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LATITUDE, moduleName, float(latitude)))
                    if longitude is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_GEO_LONGITUDE, moduleName, float(longitude)))
#
                    
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_GPS_TRACKPOINT), attrs)
                    blackboard.indexArtifact(art)
'''
    elif plugin_type == "Mobile":
        artifact_block = r'''
#                     ## ---- MOBILE / TSK_CONTACT ----
                    firstname = row.get("firstname") or ""
                    lastname  = row.get("lastname") or ""
                    phone     = row.get("phone")
                    bt_mac    = row.get("bt_mac")
#
                    full_name = (firstname + " " + lastname).strip()
#
                    attrs = ArrayList()
                    if full_name:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, full_name))
                    if phone:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER, moduleName, phone))
                    if bt_mac:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, bt_mac))
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_CONTACT), attrs)
                    blackboard.indexArtifact(art)
'''

    elif plugin_type == "Bluetooth":
        artifact_block = r'''
#                     ## ---- Bluetooth / TSK_BLUETOOTH_PAIRING ----
                    devicename = row.get("devicename") or ""
                    bt_mac    = row.get("bt_mac")
#
                    
#
                    attrs = ArrayList()
                    if devicename:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DEVICE_NAME, moduleName, devicename))
                    if bt_mac:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, bt_mac))
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_BLUETOOTH_PAIRING), attrs)
                    blackboard.indexArtifact(art)
'''
    else:
        artifact_block = r'''
#                     ## ---- CALL / TSK_CALLLOG ----
                    caller           = row.get("caller") or ""
                    caller_mac       = row.get("caller_mac")
                    callee           = row.get("callee") or ""
                    callee_number    = row.get("callee_number")
                    duration_seconds = row.get("duration_seconds")
                    timestamp_epoch = row.get("timestamp_epoch")   #int UTC, recommended for TSK_DATETIME
                    timestamp_str   = row.get("timestamp_str")     #optional string for debugging
#
                    attrs = ArrayList()
                    if timestamp_epoch is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_DATETIME, moduleName, long(timestamp_epoch)))
                    if caller:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, caller))
                    if caller_mac:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_MAC_ADDRESS, moduleName, caller_mac))
                    if callee:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_NAME_PERSON, moduleName, callee))
                    if callee_number:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_PHONE_NUMBER, moduleName, callee_number))
                    if duration_seconds is not None:
                        attrs.add(BlackboardAttribute(BlackboardAttribute.ATTRIBUTE_TYPE.TSK_COMMENT, moduleName, "Duration (s): %s" % str(duration_seconds)))
#
                    art = file.newDataArtifact(BlackboardArtifact.Type(BlackboardArtifact.ARTIFACT_TYPE.TSK_CALLLOG), attrs)
                    blackboard.indexArtifact(art)
'''

    core = f'''
class UniversalPluginFactory(IngestModuleFactoryAdapter):
    def getModuleDisplayName(self):
        return "{plugin_type} Universal Plugin"
    def getModuleDescription(self):
        return "neuer Codeversuch"
    def getModuleVersionNumber(self):
        return "1.0"
    def isDataSourceIngestModuleFactory(self):
        return True 
    def createDataSourceIngestModule(self, options):
        return UniversalPluginModule()
#
#
class UniversalPluginModule(DataSourceIngestModule):
    def startUp(self, context):
        self.context = context
        self.moduleName = "{plugin_type} Universal Plugin"
#
    def process(self, dataSource, progressBar):
        case = Case.getCurrentCase()
        fm = case.getServices().getFileManager()
        blackboard = case.getServices().getBlackboard()
#
        #1) Datei im Case anhand Dateiname finden
        files = fm.findFiles(dataSource, FILENAME)
        if files is None or files.isEmpty():
            return IngestModule.ProcessResult.OK
#
        #2) Jede gefundene Datei verarbeiten
        for file in files:
            try:
                #2a) In Modul-Verzeichnis als Tempfile schreiben
                moduleDir = case.getModuleDirectory()
                tempDir = os.path.join(Case.getCurrentCase().getTempDirectory(), "{plugin_type}_Universal")
                try:
                    os.mkdir(tempDir)
                except:
                    pass
                outFile = os.path.join(tempDir, file.getName())
                ContentUtils.writeToFile(file, File(outFile))
                #2b) Meta-Pfad auf Tempfile umbiegen
                meta = dict(META)
                meta["path"] = outFile
        #
                #2c) Parsen (ohne externe Imports; parse_rows ist oben embedded)
                rows = parse_rows(meta, MAPPING, "{plugin_type}")
        #
                moduleName = self.moduleName
                for row in rows:
{artifact_block}
        #
            except Exception as e:
                #Optional: Ingest Message
                IngestServices.getInstance().postMessage(IngestMessage.createErrorMessage(self.moduleName, "Error processing " + file.getName(), str(e)))
                pass
#
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
        self.title("Autopsy Universal Wizard (CSV + SQLite + Regex)")
        self.geometry("1150x860")

        self.meta = {}
        self.preview = None
        self.mapping_widgets = {}

        self.plugin_type = tk.StringVar(value="Geo-Track")
        self.csv_sep = tk.StringVar(value=",")
        self.embed_parser = tk.BooleanVar(value=True)

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

    def choose_csv(self):
        path = filedialog.askopenfilename(
            title="CSV auswählen",
            filetypes=[("CSV/TSV/Text", "*.csv;*.tsv;*.txt"), ("Alle Dateien", "*.*")]
        )
        if not path:
            return
        self.meta = {"source_type": "csv", "path": path, "sep": self.csv_sep.get()}
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
        messagebox.showinfo("Regex-Datei gewählt", path)

    # ---------------------------
    # Preview
    # ---------------------------

    def load_preview(self):
        if not self.meta:
            messagebox.showerror("Fehler", "Keine Quelle ausgewählt.")
            return

        st = self.meta["source_type"]
        try:
            if st == "csv":
                self.meta["sep"] = self.csv_sep.get()
                df = load_csv_preview(self.meta["path"], self.meta["sep"])
            elif st == "sqlite":
                df = load_sqlite_preview(self.meta["path"], self.meta.get("query"))
            elif st == "regex":
                df = load_regex_preview(self.meta["path"], self.meta["regex"])
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
            mapping = self.get_mapping()
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
