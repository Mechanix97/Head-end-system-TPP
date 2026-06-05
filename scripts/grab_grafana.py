#!/usr/bin/env python3
"""Captura los paneles de los dashboards de Grafana a PNG (Chrome headless).

Corre en la misma maquina (no necesita pantalla: Chrome renderiza en memoria).
Loguea en Grafana por API (cookie), y para cada dashboard captura cada panel
individualmente con la vista `d-solo`, que evita el render lazy del motor
"Scenes" de Grafana 12 (un dashboard completo deja paneles sin montar).

Las imagenes se guardan en subcarpetas por dashboard:
    <out>/<uid>/NN_titulo.png

Uso:
    python scripts/grab_grafana.py
    python scripts/grab_grafana.py --range now-30m
    python scripts/grab_grafana.py --only hes-cluster,hes-overview
"""
import argparse
import base64
import glob
import http.cookiejar
import json
import os
import re
import time
import urllib.request

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Tipos de panel que se ven mejor compactos (numeros) vs anchos (series/tablas)
COMPACT = {"stat", "gauge", "bargauge", "piechart"}
WIDE_W, WIDE_H = 1100, 520
COMPACT_W, COMPACT_H = 560, 300


def slugify(s):
    s = s or "panel"
    s = s.encode("ascii", "ignore").decode()      # quita acentos
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower()
    return s or "panel"


def dashboard_panels(json_path):
    """Devuelve [(panel_id, title, type)] de un JSON de dashboard, sin rows."""
    d = json.load(open(json_path, encoding="utf-8"))
    root = d.get("dashboard", d)
    out = []
    for p in root.get("panels", []):
        if p.get("type") == "row":
            for sp in p.get("panels", []):
                if sp.get("type") != "row":
                    out.append((sp["id"], sp.get("title", ""), sp.get("type", "")))
        else:
            out.append((p["id"], p.get("title", ""), p.get("type", "")))
    return root.get("uid"), out


def get_session_cookies(base, user, pw):
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
    data = json.dumps({"user": user, "password": pw}).encode()
    req = urllib.request.Request(
        f"{base}/login", data=data, headers={"Content-Type": "application/json"})
    resp = opener.open(req, timeout=15)
    if resp.status != 200:
        raise RuntimeError(f"login devolvio HTTP {resp.status}")
    return {c.name: c.value for c in cj}


def make_driver():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--hide-scrollbars")
    opts.add_argument("--force-device-scale-factor=1")
    opts.add_argument("--window-size=1200,700")
    return webdriver.Chrome(options=opts)


def capture_panel(driver, base, uid, panel_id, time_range, w, h, settle, path):
    driver.set_window_size(w, h)
    url = (f"{base}/d-solo/{uid}/x?orgId=1&panelId={panel_id}"
           f"&from={time_range}&to=now")
    driver.get(url)
    time.sleep(settle)
    png = driver.execute_cdp_cmd("Page.captureScreenshot", {"format": "png"})
    with open(path, "wb") as f:
        f.write(base64.b64decode(png["data"]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://100.86.94.38:6969")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="admin")
    ap.add_argument("--range", default="now-30m")
    ap.add_argument("--settle", type=float, default=7.0)
    ap.add_argument("--dash-dir", default=None,
                    help="carpeta con los JSON de dashboards")
    ap.add_argument("--out", default=None)
    ap.add_argument("--only", default=None,
                    help="uids separados por coma (default: todos)")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    dash_dir = args.dash_dir or os.path.join(
        here, "..", "metrics", "grafana", "dashboards")
    out_dir = args.out or os.path.join(
        here, "..", "..",
        "Informe-Trabajo-Practico-Profesional", "_Imagenes", "S10", "grafana")
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    files = sorted(glob.glob(os.path.join(dash_dir, "hes-*.json")))
    only = set(args.only.split(",")) if args.only else None

    print(f"Salida: {out_dir}")
    print(f"Rango : {args.range}")

    cookies = get_session_cookies(args.base, args.user, args.password)
    driver = make_driver()
    total = 0
    try:
        driver.get(f"{args.base}/login")
        time.sleep(1)
        for n, v in cookies.items():
            driver.add_cookie({"name": n, "value": v, "path": "/"})

        for f in files:
            uid, panels = dashboard_panels(f)
            if only and uid not in only:
                continue
            sub = os.path.join(out_dir, uid)
            os.makedirs(sub, exist_ok=True)
            print(f"\n{uid} ({len(panels)} paneles):")
            for idx, (pid, title, ptype) in enumerate(panels, 1):
                compact = ptype in COMPACT
                w, h = (COMPACT_W, COMPACT_H) if compact else (WIDE_W, WIDE_H)
                name = f"{idx:02d}_{slugify(title)}.png"
                path = os.path.join(sub, name)
                try:
                    capture_panel(driver, args.base, uid, pid,
                                  args.range, w, h, args.settle, path)
                    kb = os.path.getsize(path) // 1024
                    print(f"  [OK] {name} ({ptype}, {kb} KB)")
                    total += 1
                except Exception as e:
                    print(f"  [FALLO] panel {pid} '{title}': {e}")
    finally:
        driver.quit()
    print(f"\nListo. {total} paneles capturados en {out_dir}")


if __name__ == "__main__":
    main()
