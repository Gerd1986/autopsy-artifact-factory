# Autopsy Artifact Wizard

## Preprocessing-Eingaben

1. Als Quelle CSV, SQLite, Text/Regex oder DLT wählen. Dieser Typ beschreibt die Ausgabe der Preprocessing-Pipeline.
2. **Preprocessing-Eingaben wählen (alle Dateitypen)** öffnet eine Mehrfachauswahl ohne Formatfilter. Jede ausgewählte Datei durchläuft separat die Pipeline. Vorschau und lokaler Parsertest führen die Ergebnisse zusammen.
3. Pipeline konfigurieren (`{input}`, `{output}`, `{script_dir}`). **Auswahl leeren** verwendet wieder die oben gewählte Quelldatei. Bei deaktiviertem Preprocessing werden auch konfigurierte Pipeline-Schritte übersprungen.

Der Autopsy-Input-Dateiname bestimmt weiterhin die Suche im Fall. Lokale Mehrfachauswahl dient dem Testen; Autopsy verarbeitet die gefundenen Dateien einzeln.

## DLT

**DLT-Trace wählen** wählt eine `.dlt`-Datei. Für die integrierte Textvorschau und das Mapping wird COVESA `dlt-convert` benötigt. Programm über PATH bereitstellen oder über das Pfadfeld bzw. die Schaltfläche daneben auswählen. Vorschau: maximal 50 Nachrichten. Spalten: `Message` (laufende Nummer), `Text` (vollständige dekodierte Nachricht einschließlich Header). Der lokale Parser verarbeitet alle Nachrichten; der eingebettete Autopsy-Parser verwendet denselben Konverter. Der Konverter muss auch auf dem Autopsy-Rechner erreichbar sein.

**DLT Viewer öffnen** startet den installierten COVESA DLT Viewer mit dem Original-Trace (bei Mehrfachauswahl dem ersten Trace). Den Programmpfad im Feld DLT Viewer auswählen. Der Viewer läuft als eigenes Fenster; er wird nicht in Tk eingebettet. Proprietäre Non-Verbose-Dekodierung benötigt passende Projekt-/Decoder-Konfiguration im Viewer.

Offizielle Werkzeuge:
- https://github.com/COVESA/dlt-daemon/blob/master/doc/dlt-convert.1.md
- https://github.com/COVESA/dlt-viewer

## Start und Tests

Python 3 mit pandas: `python wizard.py`.
Tests: `python -m unittest -v`.
