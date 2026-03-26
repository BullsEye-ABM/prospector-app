# 🎯 Prospector App

Pipeline de prospección B2B automatizado. Genera listas de prospectos calificados con email y teléfono validados.

**Stack:** Claude AI · Clay · Lusha · Lemlist · Streamlit

---

## Despliegue en Streamlit Cloud (URL pública gratuita)

### Paso 1 — Subir el código a GitHub

1. Ve a [github.com](https://github.com) e inicia sesión (o crea una cuenta gratis)
2. Clic en **New repository** → nombre: `prospector-app` → **Create**
3. En la página del repo vacío, elige **"uploading an existing file"**
4. Arrastra TODOS los archivos de esta carpeta y confirma el upload

> La estructura debe quedar así en GitHub:
> ```
> prospector-app/
> ├── app.py
> ├── requirements.txt
> └── .streamlit/
>     └── config.toml
> ```

### Paso 2 — Conectar con Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io) e inicia sesión con tu cuenta de GitHub
2. Clic en **New app**
3. Selecciona el repositorio `prospector-app`
4. En **Main file path** escribe: `app.py`
5. Clic en **Deploy**

### Paso 3 — Agregar las API Keys (Secrets)

1. En Streamlit Cloud, abre tu app y ve a ⚙️ **Settings → Secrets**
2. Pega esto y reemplaza con tus keys reales:

```toml
ANTHROPIC_API_KEY         = "sk-ant-TU_KEY"
CLAY_API_KEY              = "TU_KEY"
CLAY_COMPANIES_TABLE_ID   = "TU_TABLE_ID"
CLAY_CONTACTS_TABLE_ID    = "TU_TABLE_ID"
LUSHA_API_KEY             = "TU_KEY"
LEMLIST_API_KEY           = "TU_KEY"
```

3. Clic en **Save** → la app se reinicia automáticamente
4. Desactiva **Modo Demo** en el sidebar → listo 🎉

---

## Cómo compartir con el equipo

Copia la URL de tu app (algo como `https://tu-usuario-prospector-app-xxxx.streamlit.app`) y compártela. Cualquiera con el link puede usarla directamente desde el navegador.

---

## Configuración de Clay (una sola vez)

Antes de usar la app en modo producción, crea estas dos tablas en Clay:

**Tabla 1 — "Empresas Target"**
Columnas: Company Name, Domain, LinkedIn URL, Country, Industry, Fit Reason

**Tabla 2 — "Contactos Enriquecidos"**
- Agrega integración **People Search** (Apollo o LinkedIn)
- Filtra por los cargos de tu Buyer Persona
- Activa enriquecimiento de **email** (Hunter, Apollo, etc.)

Copia los IDs de ambas tablas y agrégalos como Secrets en Streamlit Cloud.

---

## Correr localmente (opcional)

```bash
pip install -r requirements.txt
streamlit run app.py
```
