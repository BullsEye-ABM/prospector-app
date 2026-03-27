"""
╔══════════════════════════════════════════════════════════════════╗
║          PROSPECTOR AGENT — BullsEye                            ║
║  Corre el flujo completo de prospección automáticamente          ║
║  para un cliente específico.                                     ║
║                                                                  ║
║  Uso:                                                            ║
║    python agent.py                          → menú interactivo   ║
║    python agent.py --cliente "weCAD4you"    → cliente específico ║
║    python agent.py --todos                  → todos los clientes ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os, sys, json, argparse, time
from datetime import datetime

# ── Cargar secrets.toml ────────────────────────────────────────────────────────
def _load_secrets():
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError:
            import toml as tomllib
            return tomllib.load(open(".streamlit/secrets.toml"))
    with open(".streamlit/secrets.toml", "rb") as f:
        return tomllib.load(f)

try:
    _secrets = _load_secrets()
    SUPABASE_URL      = _secrets.get("SUPABASE_URL", "")
    SUPABASE_KEY      = _secrets.get("SUPABASE_KEY", "")
    ANTHROPIC_API_KEY = _secrets.get("ANTHROPIC_API_KEY", "")
    LUSHA_API_KEY     = _secrets.get("LUSHA_API_KEY", "")
except Exception as e:
    print(f"❌ No se pudo cargar secrets.toml: {e}")
    sys.exit(1)

# ── Imports ────────────────────────────────────────────────────────────────────
import requests
import anthropic

# ── Supabase client simple ─────────────────────────────────────────────────────
class SupabaseAgent:
    def __init__(self, url, key):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }

    def get(self, table, params=None):
        r = requests.get(f"{self.url}/rest/v1/{table}",
                         headers=self.headers, params=params or {})
        r.raise_for_status()
        return r.json()

    def patch(self, table, match: dict, data: dict):
        params = {k: f"eq.{v}" for k, v in match.items()}
        r = requests.patch(f"{self.url}/rest/v1/{table}",
                           headers=self.headers, params=params, json=data)
        r.raise_for_status()
        return r.json()

    def post(self, table, data: dict):
        r = requests.post(f"{self.url}/rest/v1/{table}",
                          headers=self.headers, json=data)
        r.raise_for_status()
        return r.json()


db = SupabaseAgent(SUPABASE_URL, SUPABASE_KEY)
claude = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


# ── Funciones del agente ───────────────────────────────────────────────────────

def _parse_json_field(val):
    if isinstance(val, (dict, list)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return {}


def log(msg, emoji="▶"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {emoji}  {msg}")


def get_clients():
    """Obtiene todos los clientes de Supabase."""
    return db.get("clients", {"order": "name"})


def recomendar_empresas(cliente: dict, n: int = 20) -> list:
    """Usa Claude para recomendar empresas según el ICP del cliente."""
    icp      = _parse_json_field(cliente.get("icp")) or {}
    bp       = _parse_json_field(cliente.get("buyer_persona")) or {}
    pv       = _parse_json_field(cliente.get("propuesta_de_valor")) or {}
    excl_dom = set(cliente.get("processed_domains") or [])
    excl_com = [e.get("nombre_empresa","") for e in
                (_parse_json_field(cliente.get("exclusion_companies")) or [])]

    excl_txt = ""
    if excl_dom:
        excl_txt += f"Dominios ya prospectados (excluir): {', '.join(list(excl_dom)[:50])}\n"
    if excl_com:
        excl_txt += f"Empresas excluidas: {', '.join(excl_com[:30])}\n"

    prompt = (
        f"Eres un experto en prospección B2B. Recomienda {n} empresas reales "
        f"que encajen perfectamente con este perfil de cliente ideal.\n\n"
        f"Propuesta de valor del cliente: {pv.get('propuesta','')}\n"
        f"Industrias objetivo: {', '.join(icp.get('industrias',[]))}\n"
        f"Geografías: {', '.join(icp.get('geografias',[]))}\n"
        f"Tamaño empresa: {icp.get('tamano_empresa',{})}\n"
        f"Modelo negocio: {icp.get('modelo_negocio','B2B')}\n"
        f"Exclusiones: {', '.join(icp.get('exclusiones',[]))}\n"
        f"{excl_txt}\n"
        f"Devuelve SOLO un JSON válido: lista de {n} empresas con campos: "
        f"nombre_empresa, dominio_web, industria, pais, tamano_empleados, razon_fit, linkedin_url.\n"
        f"No repitas empresas excluidas. No agregues texto antes ni después del JSON."
    )

    resp = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        import re
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)
    return json.loads(raw)


def auto_revisar_empresas(empresas: list, cliente: dict) -> list:
    """Claude revisa y aprueba/rechaza cada empresa según el ICP."""
    icp  = _parse_json_field(cliente.get("icp")) or {}
    pv   = _parse_json_field(cliente.get("propuesta_de_valor")) or {}
    rech = [r.get("razon_rechazo","") for r in
            (_parse_json_field(cliente.get("empresas_rechazadas")) or [])
            if r.get("razon_rechazo")]

    empresas_txt = json.dumps([{
        "nombre": e.get("nombre_empresa",""), "dominio": e.get("dominio_web",""),
        "industria": e.get("industria",""),   "pais": e.get("pais",""),
        "empleados": e.get("tamano_empleados",""), "razon_fit": e.get("razon_fit",""),
    } for e in empresas], ensure_ascii=False, indent=2)

    rechazos_txt = ("\nRazones de rechazo anteriores (aprende del patrón):\n" +
                    "\n".join(f"- {r}" for r in rech[:15]) if rech else "")

    prompt = (
        "Eres un agente experto en prospección B2B. Revisa estas empresas candidatas "
        "y decide cuáles aprobar o rechazar según el ICP.\n\n"
        f"Propuesta de valor: {pv.get('propuesta','')}\n"
        f"Industrias: {', '.join(icp.get('industrias',[]))}\n"
        f"Geografías: {', '.join(icp.get('geografias',[]))}\n"
        f"Tamaño: {icp.get('tamano_empresa',{})}\n"
        f"Exclusiones: {', '.join(icp.get('exclusiones',[]))}\n"
        f"{rechazos_txt}\n\n"
        f"Empresas:\n{empresas_txt}\n\n"
        "Sé estricto: solo aprueba si claramente encaja con el ICP.\n"
        'Devuelve SOLO un JSON: [{"dominio":"...", "aprobada": true/false, "razon":"..."}, ...]\n'
        "No agregues texto antes ni después del JSON."
    )

    resp = claude.messages.create(
        model="claude-opus-4-5",
        max_tokens=2000,
        messages=[{"role": "user", "content": prompt}]
    )
    raw = resp.content[0].text.strip()
    if raw.startswith("```"):
        import re
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw)

    decisiones = json.loads(raw)
    dec_map = {d.get("dominio","").lower(): d for d in decisiones}
    for e in empresas:
        dec = dec_map.get(e.get("dominio_web","").lower(), {})
        e["aprobada"]     = dec.get("aprobada", True)
        e["razon_agente"] = dec.get("razon", "")
    return empresas


def guardar_empresas_activas(cliente_id: str, empresas: list):
    """Guarda las empresas aprobadas en Supabase."""
    db.patch("clients", {"id": cliente_id}, {"empresas_activas": json.dumps(empresas)})


def run_cliente(cliente: dict, n_empresas: int = 20):
    """
    Ejecuta el flujo completo del agente para un cliente:
    1. Recomienda empresas con IA
    2. Revisa y aprueba/rechaza automáticamente
    3. Guarda en Supabase
    4. Muestra resumen y próximos pasos manuales
    """
    nombre = cliente.get("name", "Sin nombre")
    log(f"Iniciando agente para cliente: {nombre}", "🎯")

    # ── Paso 1: Recomendar empresas ─────────────────────────────────────────────
    log(f"Recomendando {n_empresas} empresas con IA...", "🔍")
    try:
        empresas = recomendar_empresas(cliente, n=n_empresas)
        log(f"Claude recomendó {len(empresas)} empresas", "✅")
    except Exception as e:
        log(f"Error al recomendar empresas: {e}", "❌")
        return

    # ── Paso 2: Revisar automáticamente ────────────────────────────────────────
    log("Agente revisando qué empresas aprobar...", "🤖")
    try:
        empresas = auto_revisar_empresas(empresas, cliente)
        aprobadas  = [e for e in empresas if e.get("aprobada")]
        rechazadas = [e for e in empresas if not e.get("aprobada")]
        log(f"Resultado: {len(aprobadas)} aprobadas, {len(rechazadas)} rechazadas", "📊")
    except Exception as e:
        log(f"Error en revisión automática: {e}", "❌")
        aprobadas = empresas  # fallback: aprobar todas

    # ── Paso 3: Guardar en Supabase ─────────────────────────────────────────────
    log("Guardando empresas en Supabase...", "💾")
    try:
        guardar_empresas_activas(cliente["id"], empresas)
        log("Guardado exitoso", "✅")
    except Exception as e:
        log(f"Error guardando en Supabase: {e}", "❌")

    # ── Paso 4: Resumen ─────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"  RESUMEN — {nombre}")
    print("=" * 60)
    print(f"  ✅ Empresas aprobadas : {len(aprobadas)}")
    print(f"  ❌ Empresas rechazadas: {len(rechazadas)}")
    print()
    print("  Empresas APROBADAS:")
    for e in aprobadas:
        print(f"    • {e.get('nombre_empresa','')} ({e.get('pais','')}) — {e.get('razon_agente','')[:80]}")
    print()
    print("  ⚡ PRÓXIMO PASO MANUAL (3 min):")
    print(f"  → Abre la app, selecciona {nombre}, ve a Empresas")
    print(f"  → Las empresas ya están pre-aprobadas por el agente")
    print(f"  → Ajusta si quieres, luego abre Sales Navigator y usa el plugin de Lemlist")
    print("=" * 60)
    print()

    return {"aprobadas": len(aprobadas), "rechazadas": len(rechazadas), "empresas": empresas}


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prospector Agent — BullsEye")
    parser.add_argument("--cliente", type=str, help="Nombre del cliente a prospectar")
    parser.add_argument("--todos",   action="store_true", help="Correr para todos los clientes")
    parser.add_argument("--n",       type=int, default=20, help="Número de empresas a recomendar")
    args = parser.parse_args()

    print()
    print("╔══════════════════════════════════════════╗")
    print("║     🎯 PROSPECTOR AGENT — BullsEye       ║")
    print("╚══════════════════════════════════════════╝")
    print()

    # Cargar clientes
    try:
        clientes = get_clients()
        log(f"Cargados {len(clientes)} clientes desde Supabase", "📋")
    except Exception as e:
        log(f"Error conectando a Supabase: {e}", "❌")
        sys.exit(1)

    if not clientes:
        log("No hay clientes registrados en Supabase", "⚠️")
        sys.exit(0)

    if args.todos:
        # Correr para todos
        log(f"Corriendo agente para {len(clientes)} clientes...", "🚀")
        for c in clientes:
            run_cliente(c, n_empresas=args.n)
            time.sleep(2)  # pausa entre clientes

    elif args.cliente:
        # Cliente específico
        match = [c for c in clientes if args.cliente.lower() in c.get("name","").lower()]
        if not match:
            log(f"No se encontró cliente '{args.cliente}'", "❌")
            print("Clientes disponibles:")
            for c in clientes:
                print(f"  • {c.get('name','')}")
            sys.exit(1)
        run_cliente(match[0], n_empresas=args.n)

    else:
        # Menú interactivo
        print("Clientes disponibles:")
        for i, c in enumerate(clientes):
            print(f"  {i+1}. {c.get('name','')}")
        print()
        try:
            sel = int(input("Selecciona un cliente (número): ")) - 1
            if 0 <= sel < len(clientes):
                run_cliente(clientes[sel], n_empresas=args.n)
            else:
                log("Número inválido", "❌")
        except (ValueError, KeyboardInterrupt):
            log("Cancelado", "⚠️")
