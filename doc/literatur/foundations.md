# Abstract

* GraphBLAS: Kernmenge von matrixbasierten Graphalgorithmen
* Graphdarstellung über Adjazenz- und Inzidenzmatrizen
* Adjazenzmatrizen meist leichter zu analysieren, Inzidenzmatrizen meist besser zur Datendarstellung
* Beide Typen leicht über Matrixmultiplikation zu verbinden
* geringe Menge an Matrixoperationen ausreichend für große Vielfalt an Graphen

# Einführung

* Graphen eine der wichtigsten Datenstrukturen, für Vielzahl an Problemen gut geeignet
* Graphalgorithmen meist schwer zu paralellisieren
* Ein Zeil des Standards: Einheitliche Schnittstelle für Hard- und Softwareweiterentwicklung

# Adjazenzmatrizen

* Zeilen und Spalten sind Knoten, Werte dazwischen ungleich 0 stehen für Kanten (mit Gewicht)
* Eigenschaften:
  * gerichtet (Kantenrichtung)
  * gewichtet (Kantengewicht)
  * bipartit (falls Matrix nicht quadratisch, damit Menge an Knoten mit ausgehenden Kanten anders als Menge mit eingehenden)

# Inzidenzmatrizen

* Jede Zeile entspricht einer Kante, jede Spalte einem Knoten
* können auch gut Multi- und Hypergraphen darstellen (im Gegensatz zu Adjazenzmatrizen)
* Eigenschaften
  * gerichtet
  * gewichtet
  * multipartit
  * multi (mehrere Kanten zwischen zwei Knoten)
  * hyper (eine Kante verbindet mehr als zwei Knoten)

# Skalaroperationen

* Operatoren sollten Teil einer Algebra sein
  * kommutativ
  * assoziativ
  * distributiv

# Matrixeigenschaften

* Unterschied zwischen elementweiser und gewöhnlicher Matrixmultiplikation beachten
* Matrixmultiplikation nur kommutativ mithilfe der Transposition

# 0-Element: Keine Kante

* Reduktion des benötigten Speicherplatzes in dem 0-Kanten nicht gespeichert werden -> Sparse Matrix
