#!/usr/bin/env python3
"""Reducer (Hadoop Streaming): suma los 1 de cada clave.

Hadoop entrega las líneas ordenadas por clave, así que basta con acumular
mientras la clave no cambie.
"""
import sys

clave_actual, suma = None, 0
for linea in sys.stdin:
    linea = linea.strip()
    if not linea:
        continue
    clave, valor = linea.split("\t", 1)
    if clave != clave_actual:
        if clave_actual is not None:
            print(f"{clave_actual}\t{suma}")
        clave_actual, suma = clave, 0
    suma += int(valor)

if clave_actual is not None:
    print(f"{clave_actual}\t{suma}")

