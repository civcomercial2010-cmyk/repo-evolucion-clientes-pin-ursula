from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd


ROOT = Path(r"C:\Users\User\Desktop\Propulse IA Repositorio Proyectos\HPIN\Repo HPIN\Repo evolucion clientes")
HIST_FILE = Path(r"C:\Users\User\Downloads\Ursula Moreno - BBDD historico evolucion.xlsx")
ACTUAL_FILE = Path(r"C:\Users\User\Downloads\consumo 1t26 ursula.xlsx")
OUT_FILE = ROOT / "data" / "clientes_canonico.json"
OUT_JS_FILE = ROOT / "data" / "clientes_canonico.js"


def normalizar_nombre(valor: str) -> str:
    txt = str(valor or "").upper().strip()
    txt = re.sub(r"\s+", " ", txt)
    txt = txt.replace(".", "").replace(",", "")
    return txt


def cargar_historico() -> pd.DataFrame:
    raw = pd.read_excel(HIST_FILE, sheet_name="Copia de EVOLUCION", header=None)
    # Cabecera funcional conocida: Cliente | Ventas 1T25 | Ventas 2T25 | Ventas 3T25 | Ventas 4T25
    table = raw.iloc[:, :5].copy()
    table.columns = ["nombre_historico", "v_1t25", "v_2t25", "v_3t25", "v_4t25"]
    table = table.iloc[2:].copy()
    table = table[table["nombre_historico"].notna()]
    table = table[~table["nombre_historico"].astype(str).str.contains("INTRUCCIONES|Intrucciones|Total clientes|Clientes", case=False, na=False)]
    for c in ["v_1t25", "v_2t25", "v_3t25", "v_4t25"]:
        table[c] = pd.to_numeric(table[c], errors="coerce").fillna(0.0)
    table["nombre_norm"] = table["nombre_historico"].apply(normalizar_nombre)
    return table[["nombre_historico", "nombre_norm", "v_1t25", "v_2t25", "v_3t25", "v_4t25"]]


def cargar_catalogo() -> pd.DataFrame:
    cat = pd.read_excel(ACTUAL_FILE, sheet_name="total clientes")
    def pick(pattern: str):
        for c in cat.columns:
            if re.search(pattern, str(c), re.IGNORECASE):
                return c
        return None

    col_codigo = pick(r"^N")
    col_nombre = pick(r"Nombre")
    col_grupo = pick(r"Grupo\s*precio")
    if col_codigo is None or col_nombre is None or col_grupo is None:
        raise ValueError("No se pudieron identificar columnas clave en hoja 'total clientes'.")

    out = pd.DataFrame(
        {
            "codigo_cliente": cat[col_codigo].astype(str).str.strip(),
            "nombre_actual": cat[col_nombre].astype(str).str.strip(),
            "grupo_raw": cat[col_grupo],
        }
    )
    out["grupo_num"] = pd.to_numeric(out["grupo_raw"], errors="coerce")
    out["grupo_cliente"] = out["grupo_num"].map({1.0: "1", 2.0: "2"}).fillna("sin_grupo")
    out["nombre_norm"] = out["nombre_actual"].apply(normalizar_nombre)
    return out[["codigo_cliente", "nombre_actual", "nombre_norm", "grupo_cliente"]]


def cargar_1t26_desde_extracto() -> pd.DataFrame:
    raw = pd.read_excel(ACTUAL_FILE, sheet_name="consumo 1T26", header=None)
    # Cliente: "4300002572 - NOMBRE CLIENTE"
    patron_cliente = re.compile(r"^\s*(\d{8,12})\s*-\s*(.+?)\s*$")
    # Producto: "012BASES099", "3MALLA001"... (sin signos de puntuación).
    patron_producto = re.compile(r"^\d[0-9A-ZÁÉÍÓÚÑ]{5,}$")
    rows: list[dict] = []
    cliente_actual = None
    productos_actuales: list[dict] = []

    for _, row in raw.iterrows():
        a = row.iloc[0]
        b = row.iloc[1] if len(row) > 1 else None
        e = row.iloc[4] if len(row) > 4 else None
        texto = "" if pd.isna(a) else str(a).strip()
        m = patron_cliente.match(texto)
        if m:
            if cliente_actual is not None:
                total_actual = round(sum(p["importe"] for p in productos_actuales), 2)
                rows.append(
                    {
                        "codigo_cliente": cliente_actual["codigo_cliente"],
                        "nombre_consumo": cliente_actual["nombre_consumo"],
                        "venta_1t26": total_actual,
                        "consumos_1t26": productos_actuales,
                    }
                )
            cliente_actual = {"codigo_cliente": m.group(1), "nombre_consumo": m.group(2).strip()}
            productos_actuales = []
            continue
        if cliente_actual is not None:
            cantidad = pd.to_numeric(row.iloc[2] if len(row) > 2 else None, errors="coerce")
            importe = pd.to_numeric(e, errors="coerce")
            codigo_producto = re.sub(r"\s+", "", texto.upper())
            if pd.notna(importe) and patron_producto.match(codigo_producto):
                productos_actuales.append(
                    {
                        "codigo_producto": codigo_producto,
                        "cantidad": 0.0 if pd.isna(cantidad) else round(float(cantidad), 2),
                        "descripcion": "" if pd.isna(b) else str(b).strip(),
                        "importe": round(float(importe), 2),
                    }
                )

    if cliente_actual is not None:
        total_actual = round(sum(p["importe"] for p in productos_actuales), 2)
        rows.append(
            {
                "codigo_cliente": cliente_actual["codigo_cliente"],
                "nombre_consumo": cliente_actual["nombre_consumo"],
                "venta_1t26": total_actual,
                "consumos_1t26": productos_actuales,
            }
        )
    return pd.DataFrame(rows)


def cargar_total_1t26_hoja() -> float:
    raw = pd.read_excel(ACTUAL_FILE, sheet_name="consumo 1T26", header=None)
    for _, row in raw.iterrows():
        texto = "" if pd.isna(row.iloc[0]) else str(row.iloc[0]).strip().upper()
        if texto == "TOTAL":
            importe = pd.to_numeric(row.iloc[4] if len(row) > 4 else None, errors="coerce")
            if pd.notna(importe):
                return round(float(importe), 2)
    return 0.0


def construir_dataset() -> dict:
    hist = cargar_historico()
    cat = cargar_catalogo()
    c26 = cargar_1t26_desde_extracto()
    total_1t26_hoja = cargar_total_1t26_hoja()

    base = cat.merge(c26, on="codigo_cliente", how="left")
    base["venta_1t26"] = base["venta_1t26"].fillna(0.0)

    # Enlace de histórico por nombre normalizado.
    base = base.merge(hist, on="nombre_norm", how="left")
    for c in ["v_1t25", "v_2t25", "v_3t25", "v_4t25"]:
        base[c] = base[c].fillna(0.0)

    registros = []
    for _, r in base.iterrows():
        registros.append(
            {
                "codigo_cliente": r["codigo_cliente"],
                "nombre_cliente": r["nombre_actual"],
                "grupo_cliente": r["grupo_cliente"],
                "nombre_fuente_historico": None if pd.isna(r.get("nombre_historico")) else r.get("nombre_historico"),
                "nombre_fuente_actual": None if pd.isna(r.get("nombre_consumo")) else r.get("nombre_consumo"),
                "ventas": {
                    "1T25": round(float(r["v_1t25"]), 2),
                    "2T25": round(float(r["v_2t25"]), 2),
                    "3T25": round(float(r["v_3t25"]), 2),
                    "4T25": round(float(r["v_4t25"]), 2),
                    "1T26": round(float(r["venta_1t26"]), 2),
                },
                "consumos_1t26": r.get("consumos_1t26") if isinstance(r.get("consumos_1t26"), list) else [],
            }
        )

    return {
        "metadata": {
            "fuente_historico": str(HIST_FILE),
            "fuente_actual": str(ACTUAL_FILE),
            "hoja_historico": "Copia de EVOLUCION",
            "hoja_catalogo": "total clientes",
            "hoja_consumo": "consumo 1T26",
            "total_1t26_hoja": total_1t26_hoja,
            "version": "1.0.0",
        },
        "clientes": registros,
    }


def main() -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    dataset = construir_dataset()
    OUT_FILE.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_JS_FILE.write_text(
        "window.DASHBOARD_DATA = " + json.dumps(dataset, ensure_ascii=False) + ";",
        encoding="utf-8",
    )
    print(f"OK -> {OUT_FILE}")
    print(f"OK -> {OUT_JS_FILE}")
    print(f"Clientes: {len(dataset['clientes'])}")


if __name__ == "__main__":
    main()
