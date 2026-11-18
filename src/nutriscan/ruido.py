"""Daña a propósito el texto de ingredientes para medir qué tan robusto es un modelo.

Simula lo que pasa con una foto mal tomada o un OCR imperfecto:
- "errores": cada palabra, con probabilidad `nivel`, sufre un error de letra
  (se borra, se cambia o se intercambia una letra) o desaparece.
- "truncado": se pierde el final de la lista (solo queda la fracción 1 - nivel),
  como cuando la foto corta la etiqueta.

Es determinista: el mismo texto con la misma semilla da siempre el mismo resultado,
para que los experimentos sean reproducibles.
"""

import random
import zlib

LETRAS = "abcdefghijklmnopqrstuvwxyz"


def _error_de_letra(palabra: str, rnd: random.Random) -> str:
    if len(palabra) < 2:
        return ""
    i = rnd.randrange(len(palabra))
    tipo = rnd.choice(["borrar", "cambiar", "intercambiar"])
    if tipo == "borrar":
        return palabra[:i] + palabra[i + 1:]
    if tipo == "cambiar":
        return palabra[:i] + rnd.choice(LETRAS) + palabra[i + 1:]
    j = min(i + 1, len(palabra) - 1)
    letras = list(palabra)
    letras[i], letras[j] = letras[j], letras[i]
    return "".join(letras)


def degradar(texto: str | None, nivel: float, tipo: str = "errores", semilla: int = 42) -> str | None:
    if texto is None or nivel <= 0:
        return texto
    rnd = random.Random(semilla + zlib.crc32(texto.encode("utf-8")))
    palabras = texto.split()
    if tipo == "truncado":
        quedan = max(1, round(len(palabras) * (1 - nivel)))
        return " ".join(palabras[:quedan])
    salida = []
    for p in palabras:
        if rnd.random() < nivel:
            if rnd.random() < 0.25:
                continue  # la palabra desaparece
            p = _error_de_letra(p, rnd)
        if p:
            salida.append(p)
    return " ".join(salida)
