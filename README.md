# Matúš Krajčovič = VINF projekt

Téma: W6 - Anchor texts a štatistika k anchor textom. Document frequency, collection frequency.

## Použitie

V priečinku so súborom parser.py a vstupným wikipedia dumpom sa spustí skript parser.sh:

`./parser.sh wiki_dump_file output_file`

V tomto skripte sa pripravia súbory a spustí spark-submit príkaz.

Pri indexovaní zas využijem skript index.sh a za pomoci index.py skriptu a výstupu z parsovania vytvorím index:

`./index.sh parser_output_file index_name`

Pri vyhľadávaní cez search.sh skript si za pomoci search.py súboru a priečinku s indexom viem spustiť vyhľadávanie:

`./search.sh index_name`

Teraz môžem zadávať požiadavky pre vyhľadávanie.