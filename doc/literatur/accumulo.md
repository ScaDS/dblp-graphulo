<!-- https://accumulo.apache.org/1.8/accumulo_user_manual.html, 16.04.2017 -->
# 1. Einleitung

Skalierbarer strukturierter Datenspeicher basierend auf BigTable. Geschrieben in
Java, benutzt das HDFS. Automatisches Load-Balancing und Partitionierung,
Datenkompression und fingranulare Zugriffseinschränkungen.

# 2. Accumulo Design

Mächtigeres Datenmodell als einfacher Key-Value-Store.

* Tablet Server:
  * Verwaltet eine Menge von Tablets (Partition von Tabellen)
  * Schreibzugriffe bearbeiten, in WAL schreiben
  * neue Key-Value-Paare in memory sortieren
  * regelmäßiges Schreiben neuer Key-Value-Paare in HDFS
  * Recovery von Tablets die auf verlorenem TS waren
* Master:
  * Load balancing zwischen Tablet Servern
  * Verwaltung Zuordnung Tablet -> Tablet Server
  * Erstellen, Ändern und Löschen von Tabellen durch Client
  * Recovery von WALs
  * mehrere Master möglich, dann ein primärer und Backups

Tabellenpartition bei row boundaries, damit alle Key-Value-Paare die einen
"Datensatz" bilden auf gleichem Tablet Server -> kein verteiltes Locking nötig.

Tablet Server schreiben writes in WAL und MemTable. Sobald die MemTable zu groß
wird, schreibt der TS die sortierten Key-Value-Paare in das HDFS in eine
Relative Key File (RFile) -> Minor Compaction

Regelmäßiges Mergen von RFiles, um Anzahl Dateien zu verringern -> Major
Compaction

# 6: Tabellenkonfiguration

* Locality Groups
  * Getrenntes Speichern von unterschiedlichen column families
  * Trennung von oft und selten benötigten column families
  * Vermeidet unnötiges Scannen der selten benötigten
* Accumulo kann Constraints beim Einfügen auf Mutations anwenden
* Lookups mit Bloom Filter beschleunigen
* Iteratoren
  * modulare Möglichkeit, Funktionalität beim Scannen oder Komprimieren von
    Daten auf TS auszuführen
  * Filter sind spezielle Iteratoren
  * Combiner werden auf alle Werte mit selbem Key (rowID, cf und cq) angewendet
    -> entspricht reduce-Schritt
* Block Cache für zuletzt zugegriffene Daten

# 9: Tabellendesign

Sortierung nach rowID, auch Datentyp (zB. Date) nach Vorbereitung sortierbar

Indextabellen möglich, siehe Beispiel

Speichern von Entity-Attribute und Graphtabellen möglich, siehe Beispiel
