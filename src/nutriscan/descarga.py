"""Paso 1 de la ingesta: descargar el volcado CSV de Open Food Facts.

Uso (desde la raíz del repositorio):
    python -m nutriscan.descarga

- Si la descarga se corta, reintenta sola y continúa donde iba (hasta 20 veces).
- Guarda data/raw/descarga.json con la fecha, la URL y el tamaño, que sirven
  como evidencia para el informe.
"""

import json
import time
from datetime import datetime, timezone

import requests
from tqdm import tqdm

from nutriscan.config import cargar_proyecto, ruta

REINTENTOS = 20


def descargar() -> dict:
    url = cargar_proyecto()["fuente"]["csv_url"]
    destino = ruta("raw") / url.rsplit("/", 1)[-1]
    parcial = destino.with_suffix(destino.suffix + ".part")

    # "identity" = pedir el archivo tal cual (sin recomprimir), así el servidor
    # informa el tamaño y requests no lo descomprime por su cuenta.
    # Open Food Facts rechaza (403) a los clientes que no se identifican.
    tal_cual = {"Accept-Encoding": "identity", "User-Agent": "NutriScanCO/0.1 (proyecto academico)"}

    # Tamaño total y fecha de la última actualización según el servidor
    cabecera = requests.head(url, allow_redirects=True, timeout=60, headers=tal_cual)
    total = int(cabecera.headers["Content-Length"])
    fecha_servidor = cabecera.headers.get("Last-Modified")

    if destino.exists() and destino.stat().st_size == total:
        print(f"Ya estaba descargado: {destino}")
    else:
        for intento in range(1, REINTENTOS + 1):
            ya = parcial.stat().st_size if parcial.exists() else 0
            if ya >= total:
                break
            # If-Range: si el archivo del servidor cambió, se recibe completo (200)
            # en vez de solo lo que falta (206), y se empieza de cero.
            encabezados = {**tal_cual, "Range": f"bytes={ya}-", "If-Range": fecha_servidor} if ya else tal_cual
            try:
                with requests.get(url, headers=encabezados, stream=True, timeout=60) as r:
                    r.raise_for_status()
                    if r.status_code == 200:
                        # Respuesta completa: el tamaño y la fecha válidos son los de
                        # ESTA respuesta (el archivo pudo cambiar desde el HEAD).
                        ya = 0
                        total = int(r.headers["Content-Length"])
                        fecha_servidor = r.headers.get("Last-Modified", fecha_servidor)
                    with open(parcial, "ab" if ya else "wb") as f, tqdm(
                        total=total, initial=ya, unit="B", unit_scale=True, desc="Descargando"
                    ) as barra:
                        for bloque in r.iter_content(chunk_size=1 << 20):  # 1 MB
                            f.write(bloque)
                            barra.update(len(bloque))
            except requests.exceptions.RequestException as e:
                # Si se cae la conexión, se espera un poco y se continúa donde iba
                print(f"Intento {intento}/{REINTENTOS} interrumpido ({type(e).__name__}); reintentando...")
                time.sleep(10)
        if parcial.stat().st_size != total:
            raise RuntimeError("La descarga quedó incompleta; vuelve a ejecutar el script.")
        parcial.rename(destino)

    meta = {
        "url": url,
        "archivo": destino.name,
        "bytes_comprimido": total,
        "gb_comprimido": round(total / 1e9, 2),
        "ultima_actualizacion_servidor": fecha_servidor,
        "fecha_descarga_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    (ruta("raw") / "descarga.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    print(json.dumps(meta, indent=2, ensure_ascii=False))
    return meta


if __name__ == "__main__":
    descargar()
