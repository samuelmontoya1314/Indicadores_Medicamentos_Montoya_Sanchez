"""Fase 8b - Tablero HTML interactivo (Plotly) que lee SOLO la zona de servicio.

Genera dashboard/tablero.html y docs/index.html (para publicarlo gratis con
GitHub Pages). No hace cálculos: solo lee las tablas ind_* de SQLite.
"""
import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

import config as C

COLOR = {"primario": "#0B5394", "secundario": "#E69138", "gris": "#B7B7B7",
         "verde": "#38761D", "fondo": "#F5F7FA"}
pio.templates.default = "plotly_white"
FUENTE_FIG = dict(family="Inter, Segoe UI, Arial", size=13, color="#1F2937")


def leer(con, tabla):
    try:
        return pd.read_sql(f"SELECT * FROM {tabla}", con)
    except Exception:
        return pd.DataFrame()


def estilo(fig, alto=420):
    fig.update_layout(height=alto, margin=dict(l=10, r=10, t=10, b=10), font=FUENTE_FIG,
                      legend=dict(orientation="h", y=-0.15), paper_bgcolor="white",
                      plot_bgcolor="white")
    return fig


def a_html(fig):
    return pio.to_html(fig, include_plotlyjs=False, full_html=False,
                       config={"displayModeBar": False, "responsive": True})


def fmt(n):
    return f"{int(n):,}".replace(",", ".")


def main():
    con = sqlite3.connect(C.SERVICE_DB)
    r = leer(con, "ind_resumen").iloc[0]
    atc = leer(con, "ind_atc_estado")
    origen = leer(con, "ind_origen_atc")
    top = leer(con, "ind_top_titulares")
    evo = leer(con, "ind_evolucion_anual")
    via = leer(con, "ind_via_administracion")
    stream = leer(con, "stream_conteos_ventana")
    lam = leer(con, "v_lambda_tiporol")
    con.close()

    # --- 1. Oferta por grupo ATC (activos vs inactivos) ---
    a = atc[atc.atc_nivel1 != "SIN_ATC"].sort_values("cum_activos")
    f1 = go.Figure([
        go.Bar(y=a.grupo_atc, x=a.cum_activos, name="Activos", orientation="h",
               marker_color=COLOR["primario"]),
        go.Bar(y=a.grupo_atc, x=a.cum_inactivos, name="Inactivos", orientation="h",
               marker_color=COLOR["gris"])])
    f1.update_layout(barmode="stack", xaxis_title="Número de CUM (presentaciones)")
    estilo(f1, 480)

    # --- 2. Dependencia de importación por grupo ATC ---
    o = origen[origen.atc_nivel1 != "SIN_ATC"].sort_values("pct_importador")
    f2 = go.Figure(go.Bar(
        y=o.grupo_atc, x=o.pct_importador, orientation="h",
        marker=dict(color=o.pct_importador, colorscale="Oranges", cmin=0, cmax=100),
        text=[f"{v:.0f} %" for v in o.pct_importador], textposition="outside",
        customdata=o[["cum_con_importador", "cum_activos"]],
        hovertemplate="%{y}<br>%{x:.1f} % importado<br>%{customdata[0]} de %{customdata[1]} CUM<extra></extra>"))
    f2.update_layout(xaxis=dict(title="% de CUM activos con rol importador", range=[0, 105]))
    estilo(f2, 480)

    # --- 3. Concentración: top titulares ---
    t = top.sort_values("cum_activos")
    f3 = go.Figure(go.Bar(
        y=t.titular, x=t.cum_activos, orientation="h", marker_color=COLOR["primario"],
        text=[f"{p:.1f} %" for p in t.pct_del_total], textposition="outside",
        hovertemplate="%{y}<br>%{x} CUM activos<extra></extra>"))
    f3.update_layout(xaxis_title="CUM activos")
    estilo(f3, 520)

    # --- 4. Evolución anual ---
    f4 = go.Figure([
        go.Bar(x=evo.anio, y=evo.registros_expedidos, name="Registros sanitarios vigentes expedidos",
               marker_color=COLOR["primario"]),
        go.Scatter(x=evo.anio, y=evo.cum_inactivados, name="CUM inactivados", mode="lines+markers",
                   line=dict(color=COLOR["secundario"], width=3), yaxis="y2")])
    f4.update_layout(yaxis=dict(title="Registros expedidos"),
                     yaxis2=dict(title="CUM inactivados", overlaying="y", side="right",
                                 showgrid=False), xaxis_title="Año")
    estilo(f4, 420)

    # --- 5. Vía de administración ---
    v = via.copy()
    if len(v) > 6:
        otras = pd.DataFrame([{"viaadministracion": "OTRAS",
                               "cum_activos": v.cum_activos.iloc[6:].sum()}])
        v = pd.concat([v.head(6), otras])
    f5 = go.Figure(go.Pie(labels=v.viaadministracion, values=v.cum_activos, hole=0.55,
                          sort=False, textinfo="percent",
                          marker=dict(colors=["#0B5394", "#3D85C6", "#6FA8DC", "#9FC5E8",
                                              "#E69138", "#F6B26B", "#CCCCCC"])))
    estilo(f5, 420)

    # --- 6. Capa rápida (streaming) + vista Lambda ---
    if not stream.empty:
        s = stream.sort_values("ventana_inicio")
        f6 = go.Figure([go.Bar(x=g.ventana_inicio.str.slice(11, 19), y=g.mensajes, name=rol)
                        for rol, g in s.groupby("tiporol")])
        f6.update_layout(barmode="stack", xaxis_title="Ventana de 30 s (hora del mensaje en Kafka)",
                         yaxis_title="Mensajes")
        estilo(f6, 360)
        g6 = a_html(f6)
    else:
        g6 = "<p>Aún no hay datos de streaming.</p>"
    tabla_lambda = lam.to_html(index=False, classes="tabla", border=0) if not lam.empty else ""

    # --- Título que responde la pregunta ---
    g = atc[atc.atc_nivel1 != "SIN_ATC"].sort_values("cum_activos", ascending=False)
    g1, g2 = g.grupo_atc.iloc[0].lower(), g.grupo_atc.iloc[1].lower()
    titulo = (f"{r.pct_cum_activos_con_importador:.0f} % de los medicamentos activos en Colombia "
              f"depende de importadores; {g1} y {g2} concentran la oferta")
    corte = str(r.fecha_corte_fuente)[:10]
    proceso = str(r.fecha_proceso)[:16].replace("T", " ")

    kpis = [
        ("CUM activos", fmt(r.cum_activos), "presentaciones comerciales vigentes"),
        ("CUM inactivos", fmt(r.cum_inactivos), "presentaciones dadas de baja"),
        ("% con importador", f"{r.pct_cum_activos_con_importador:.1f} %", "de los CUM activos"),
        ("Top 10 titulares", f"{r.pct_top10_titulares:.1f} %", "de los CUM activos"),
        ("Titulares", fmt(r.titulares_activos), "con al menos un CUM activo"),
        ("Principios activos", fmt(r.principios_activos), "distintos"),
    ]
    kpi_html = "".join(f'<div class="kpi"><div class="k-t">{a}</div><div class="k-v">{b}</div>'
                       f'<div class="k-s">{c}</div></div>' for a, b, c in kpis)

    def tarjeta(titulo_t, sub, graf, ancho=""):
        return (f'<section class="card {ancho}"><h3>{titulo_t}</h3><p class="sub">{sub}</p>'
                f'{graf}</section>')

    html = f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Medicamentos vigentes en Colombia - Tablero CUM INVIMA</title>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
<style>
 body{{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:{COLOR['fondo']};color:#1F2937}}
 header{{background:linear-gradient(120deg,#073763,#0B5394 60%,#3D85C6);color:white;padding:34px 48px 28px}}
 header .eti{{text-transform:uppercase;letter-spacing:.12em;font-size:12px;opacity:.8}}
 header h1{{margin:8px 0 10px;font-size:30px;line-height:1.25;font-weight:800;max-width:1100px}}
 header p{{margin:0;opacity:.9;max-width:1000px}}
 main{{padding:24px 48px 10px;max-width:1500px;margin:auto}}
 .kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:14px;margin-top:-50px}}
 .kpi{{background:white;border-radius:14px;padding:16px 18px;box-shadow:0 4px 18px rgba(0,0,0,.08)}}
 .k-t{{font-size:12px;text-transform:uppercase;color:#6B7280;letter-spacing:.06em}}
 .k-v{{font-size:28px;font-weight:800;color:#0B5394;margin:4px 0}}
 .k-s{{font-size:12px;color:#6B7280}}
 .grid{{display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:20px}}
 .card{{background:white;border-radius:14px;padding:18px 20px;box-shadow:0 2px 10px rgba(0,0,0,.06)}}
 .card.full{{grid-column:1/-1}}
 .card h3{{margin:0;font-size:17px}} .sub{{margin:4px 0 8px;color:#6B7280;font-size:13px}}
 .tabla{{border-collapse:collapse;font-size:13px;margin-top:10px}}
 .tabla th,.tabla td{{padding:6px 12px;border-bottom:1px solid #E5E7EB;text-align:right}}
 .tabla th{{background:#F3F4F6}}
 footer{{padding:18px 48px 30px;color:#4B5563;font-size:13px;max-width:1500px;margin:auto}}
 @media(max-width:900px){{.grid{{grid-template-columns:1fr}} header,main,footer{{padding-left:18px;padding-right:18px}}}}
</style></head><body>
<header>
 <div class="eti">Gestión de Datos Masivos · Pipeline CUM INVIMA</div>
 <h1>{titulo}</h1>
 <p>¿Cómo está compuesta la oferta de medicamentos con registro vigente en Colombia por grupo
 terapéutico, vía y origen, y qué tanto depende de importaciones y de pocos titulares?</p>
 <br><br>
</header>
<main>
 <div class="kpis">{kpi_html}</div>
 <div class="grid">
  {tarjeta("Oferta por grupo terapéutico (ATC)", "CUM activos e inactivos por grupo ATC nivel 1", a_html(f1))}
  {tarjeta("Dependencia de importación", "% de CUM activos con rol de importador, por grupo ATC", a_html(f2))}
  {tarjeta("Concentración en pocos titulares", f"Top 15 titulares por CUM activos · los 10 primeros suman {r.pct_top10_titulares:.1f} %", a_html(f3))}
  {tarjeta("Vía de administración", "Distribución de los CUM activos", a_html(f5))}
  {tarjeta("Evolución en el tiempo", "Registros sanitarios vigentes por año de expedición y CUM inactivados por año", a_html(f4), "full")}
  {tarjeta("Capa rápida (streaming) · arquitectura Lambda", "Mensajes recibidos por Kafka en ventanas de 30 s (Spark Structured Streaming). La tabla compara la vista batch (deduplicada) con la vista rápida.", g6 + tabla_lambda, "full")}
 </div>
</main>
<footer>
 <b>Fuente:</b> {r.fuente} · API: <a href="{r.url_fuente}">{r.url_fuente}</a><br>
 <b>Fecha de corte de los datos:</b> {corte} (última actualización de registros en la fuente) ·
 <b>Procesado:</b> {proceso} · Unidad de análisis: CUM (expediente + consecutivo).<br>
 <b>Autores:</b> Martín Sánchez Urrego · Samuel Montoya Garzón — Institución Universitaria Pascual Bravo (Gestión de Datos Masivos G002, 2026-2).
</footer>
</body></html>"""

    salidas = [os.path.join(C.BASE_DIR, "dashboard", "tablero.html"),
               os.path.join(C.BASE_DIR, "docs", "index.html")]
    for ruta in salidas:
        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        with open(ruta, "w", encoding="utf-8") as f:
            f.write(html)
        print("Tablero generado:", ruta)
    print("Título:", titulo)
    return 0


if __name__ == "__main__":
    sys.exit(main())

