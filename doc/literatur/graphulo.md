# Algorithmen

## Jaccard-Koeffizient

Ähnlichkeit der Nachbarschaften von zwei Knoten

Schnitt / Vereinigung

Anwendungen:
* Welche Autoren haben mit ähnlichen anderen Autoren veröffentlicht?
* Welche Publikationen (Magazine o.Ä.) haben ähnliche Autoren veröffentlicht?

## k-Truss

Gibt "Graphkern" zurück, zentrale Kanten 

Ein Graph ist ein k-Truss Graph, wenn jede Kante Teil von mindestens k-2 Dreiecken ist, kann auch leerer Graph sein

Anwendungen:
* Autoren mit großer Nachbarschaft finden
* Publikationen die viele Autoren veröffentlicht haben

## Degree Filtered Bread First Search

Suche in Graph, high degree Nodes können ausgefiltert werden

Anwendungen:
* Autorendistanz (Erdos-Zahl, wenn Zentrum bekannt)

## Non-Negative Matrix Factorization

Zusammenfassung, Auffinden von "Themen" in Graph, Einsatz zum Beispiel bei Empfehlungssystemen

Teilt eine Matrix V in zwei Matrizen W und H für die gilt: V = WH (Matrixmultiplikation)

# Klassen

* ApplyOp -> Führt Funktion auf Eintrag aus
* Reducer -> commutative and associative Reduce operation
* SKVI -> Key-Value-Iteratoren