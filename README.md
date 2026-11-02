# Autopsy Artifact Wizard

Ein Python/Tkinter-Wizard zur Erstellung von Artefakt-Parsern für **Autopsy 4.23.1**. Die Vorschau und das Spalten-Mapping werden lokal vorbereitet; der Wizard generiert daraus ein Autopsy-Modul.

## Start

Python 3 mit `pandas` installieren und den Wizard starten:

```powershell
python -m pip install pandas
python wizard.py
```

Tests (sofern im Checkout vorhanden): `python -m unittest -v`.

## Grundablauf

1. Eine **Preview-Quelle** auswählen: CSV/TSV, SQLite oder Text/Regex. Dieser Quelltyp beschreibt auch das **Format nach einem eventuell vorgeschalteten Preprocessing**.
2. Unter **Autopsy Input-Dateiname** den Namen der Datei festlegen, die das generierte Plugin im Autopsy-Fall suchen soll. Dieser Name ist **unabhängig** vom lokalen Preview-Dateinamen.
3. Unter **Modulname** einen frei wählbaren Namen eintragen. Er wird für `getModuleDisplayName()` und `self.moduleName` verwendet. Ohne Eingabe gilt der bisherige Standardname.
4. Bei Bedarf die unten beschriebene **Multistep-Preprocessing-Pipeline** konfigurieren.
5. **Vorschau laden**, Spalten den gewünschten Artefaktfeldern zuordnen und das Autopsy-Modul generieren.

## Multistep-Preprocessing bedienen

Der gesamte Pipeline-Editor befindet sich direkt im Hauptfenster unter **Preprocessing**. Die frühere Eingabe „Legacy Step 1“ und der separate Pipeline-Editor sind entfernt.

### 1. Eingaben festlegen

Normalerweise dient die oben ausgewählte Preview-Datei als Input. Optional mit **Preprocessing-Eingaben wählen (alle Dateitypen)** eine oder mehrere beliebige Dateien auswählen (z. B. Binärdateien oder proprietäre Datenbanken). Jede dieser Dateien wird **separat** durch die Pipeline geschickt; die Ergebnisse werden für die lokale Vorschau zusammengeführt. **Auswahl leeren** kehrt zur normalen Preview-Datei zurück.

Wichtig: Bei vorgeschaltetem Preprocessing wählst du als Quelle oben das **erwartete Ausgabeformat**, nicht zwingend das Format der ursprünglichen Eingabedatei. Liefert dein Skript eine CSV, verwende die CSV-Quelle und stelle das Trennzeichen passend ein.

### 2. Schritte anlegen

Auf **Hinzufügen** klicken und pro Schritt diese Felder ausfüllen:

| Feld | Bedeutung |
| --- | --- |
| **Name** | Frei wählbare Bezeichnung in der Schrittliste |
| **Command** | Auszuführender Befehl inklusive Argumenten |
| **Output-Suffix** | Dateiendung des erzeugten Outputs, z. B. `.csv` oder `.txt` |
| **shell=True** | Führt den Befehl über die Shell aus; normalerweise deaktiviert lassen |

Im Befehlsfeld stehen Platzhalter zur Verfügung:

| Platzhalter | Bedeutung |
| --- | --- |
| `{input}` | Aktuelle Eingabedatei; ab Schritt 2 der Output des vorherigen Schritts |
| `{output}` | Vom Wizard bereitgestellter Zielpfad für den Output dieses Schritts |
| `{script_dir}` | Verzeichnis der Skripte bzw. des generierten Moduls, je nach Ausführungskontext |

**Beispiel für zwei Schritte:**

Schritt 1 – Binärdaten in Text umwandeln, Output-Suffix `.txt`:

```text
python "{script_dir}/decode.py" "{input}" "{output}"
```

Schritt 2 – Text in CSV umwandeln, Output-Suffix `.csv`:

```text
python "{script_dir}/to_csv.py" "{input}" "{output}"
```

Die Beispielskripte `decode.py` und `to_csv.py` sind **keine mitgelieferten Werkzeuge**. Sie stehen beispielhaft für eigene Konverter. Die Programme müssen den angegebenen `{output}`-Pfad tatsächlich schreiben; eine Ausgabe nur auf stdout genügt nicht.

Die Reihenfolge ist:

```text
Originaldatei
   -> Schritt 1 -> .txt
   -> Schritt 2 -> .csv
   -> CSV-Vorschau / Mapping / Autopsy-Parser
```

### 3. Schritte verwalten und aktivieren

- **Bearbeiten:** markierten Schritt ändern.
- **Löschen:** markierten Schritt entfernen.
- **Hoch / Runter:** Ausführungsreihenfolge ändern.
- **Multistep-Preprocessing aktiv:** Pipeline ein- oder ausschalten. Beim Hinzufügen eines Schritts wird sie automatisch aktiviert; beim Löschen des letzten Schritts deaktiviert.

Nach Änderungen **Vorschau laden** erneut anklicken, damit die aktuelle Pipeline ausgeführt und das Mapping auf Basis der erzeugten Daten angezeigt wird.

### 4. Lokale Vorschau und Autopsy

Die Pipeline läuft für die Vorschau lokal. Das generierte Modul enthält die konfigurierte Verarbeitungskette und führt sie für passende Dateien im Autopsy-Fall aus. Das **Autopsy Input-Dateiname**-Feld bestimmt, welche Datei dort gesucht wird; die lokalen zusätzlichen Preprocessing-Eingaben dienen dem Testen.

Für externe Python-, Perl- oder EXE-Tools müssen die jeweiligen Programme und Skripte in der Zielumgebung verfügbar sein. `{script_dir}` hilft beim Referenzieren mitgelieferter Skripte. Die Preprocessing-Schritte werden nacheinander ausgeführt; ein Fehler in einem Schritt verhindert ein korrektes Folgeergebnis.

## SQLite-Hinweis

Bei SQLite-Quellen berücksichtigt der Wizard zugehörige WAL-/SHM-Dateien, soweit vorhanden. Die Unicode-Behandlung der generierten Textartefakte bewahrt Zeichen wie `ü` und entfernt gezielt das unsichtbare Zeichen U+200E.

## Nicht mehr enthalten

Die frühere **DLT-Trace-/DLT-Viewer-Integration** wurde entfernt. Ebenso existiert keine separate Legacy-Preprocessing-Eingabe mehr.
