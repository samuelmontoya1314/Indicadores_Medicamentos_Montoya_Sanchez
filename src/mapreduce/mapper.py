#!/usr/bin/env python3
"""Mapper (Hadoop Streaming): por cada sobre JSON emite <grupo ATC nivel 1>\t1.

El grupo ATC nivel 1 es la primera letra del código ATC (A, B, C, ... V).
Códigos vacíos o inválidos se agrupan como SIN_ATC. La misma regla se replica en
Spark para comparar resultados.
"""
import json
import re
import sys

PATRON_ATC = re.compile(r"^[ABCDGHJLMNPRSV][0-9]{2}")

for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
    try:
        sobre = json.loads(linea)
    except json.JSONDecodeError:
        print("ERROR_JSON\t1")
        continue
    atc = ((sobre.get("payload") or {}).get("atc") or "").strip().upper()
    grupo = atc[0] if PATRON_ATC.match(atc) else "SIN_ATC"
    print(f"{grupo}\t1")

