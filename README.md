# Genie Code Workshop — Grupo LAFISE

Workshop práctico de Databricks Genie Code adaptado para el equipo de LAFISE. Cubre 4 tracks de 105 minutos cada uno usando datos sintéticos bancarios de los 8 países donde opera LAFISE.

---

## Tracks disponibles

| Track | Descripción | Duración |
|---|---|---|
| ⚙️ Data Engineering | Pipelines PySpark, ingesta medallion, reconciliación core bancario, Jobs nocturnos | 105 min |
| 📊 BI & Analytics | SQL desde lenguaje natural, Metric Views (NPL/mora), Genie Spaces, dashboards de riesgo | 105 min |
| 🧠 Data Science & ML | Scoring crediticio, MLflow, Model Serving, applyInPandas por país/segmento, alertas SARLAFT | 105 min |
| 🛡️ Data Governance | DQ regulatorio, CLS/RLS para datos financieros, framework de auditoría, Data Academy | 105 min |

---

## Preparación del ambiente (paso a paso)

Estos pasos los ejecuta el facilitador **antes del workshop**. Tiempo estimado: 30-45 minutos.

---

### Paso 1 — Subir el notebook generador de datos al workspace

1. Descarga el archivo `generate_workshop_data.py` de este repositorio.
2. Abre el workspace de Databricks en el navegador.
3. En la barra lateral, haz clic en **Workspace** → navega a una carpeta compartida (por ejemplo `/Shared/workshop/`).
4. Haz clic en el botón **"+ Add"** (o arrastra el archivo) → **Import** → sube `generate_workshop_data.py`.
5. Databricks lo reconocerá automáticamente como notebook.

---

### Paso 2 — Crear un cluster y ejecutar el notebook

1. En la barra lateral, ve a **Compute** → **Create compute**.
2. Configura el cluster:
   - **Runtime**: 15.x ML (o superior) — necesario para el track de Data Science
   - **Node type**: cualquier nodo con al menos 16 GB RAM (e.g., `Standard_DS4_v2`)
   - **Single node** es suficiente para la generación de datos
3. Haz clic en **Create compute** y espera a que inicie (2-3 minutos).
4. Abre el notebook `generate_workshop_data` que subiste en el Paso 1.
5. En la esquina superior derecha, selecciona el cluster recién creado.
6. Haz clic en **Run all** (▶▶) o `Shift + Enter` celda por celda.
7. Al terminar, el output final debe mostrar:

```
✅ workshop.gold.dim_clientes
✅ workshop.gold.dim_sucursales
✅ workshop.gold.fact_transacciones
✅ workshop.gold.fact_cartera_creditos
✅ workshop.gold.fact_kpis_diarios
✅ Generación completa. Workshop listo.
```

> El notebook es idempotente — si algo falla, puedes ejecutarlo de nuevo sin problema.

---

### Paso 3 — Verificar las tablas en Unity Catalog

1. En la barra lateral, haz clic en **Catalog** (ícono de catálogo).
2. Navega a **workshop** → **gold**.
3. Confirma que las 5 tablas existen y tienen datos:
   - `dim_clientes` (~2,400 filas)
   - `dim_sucursales` (~136 filas)
   - `fact_transacciones` (~200K filas)
   - `fact_cartera_creditos` (~5,000 filas)
   - `fact_kpis_diarios` (~4,000 filas)

---

### Paso 4 — Dar permisos a los participantes

1. En la barra lateral, ve a **Catalog** → selecciona el catálogo **workshop**.
2. Haz clic en la pestaña **Permissions** → **Grant**.
3. Otorga los siguientes permisos al grupo o usuarios del workshop:

```sql
-- También puedes ejecutar esto en un notebook SQL
GRANT USE CATALOG ON CATALOG workshop TO `workshop_users`;
GRANT USE SCHEMA ON SCHEMA workshop.gold TO `workshop_users`;
GRANT SELECT ON ALL TABLES IN SCHEMA workshop.gold TO `workshop_users`;
```

> Reemplaza `` `workshop_users` `` por el nombre real del grupo en el workspace de LAFISE. Si no existe un grupo, agrégalo desde **Settings → Identity & Access → Groups**.

---

### Paso 5 — Habilitar Foundation Model API

Requerido para los steps que usan FMAPI (Knowledge Assistant, AI Functions en SQL, alertas SARLAFT). Si ya está habilitado en el workspace, omite este paso.

1. Ve a **Settings** (esquina inferior izquierda) → **Workspace settings**.
2. Busca **"Foundation Model APIs"** o **"External Models"**.
3. Habilítalo y confirma que el modelo `databricks-claude-sonnet-4` está disponible.
4. Para verificar, abre un notebook y ejecuta:

```python
import httpx
from databricks.sdk.config import Config
cfg = Config()
r = httpx.post(
    f"{cfg.host}/serving-endpoints/databricks-claude-sonnet-4/invocations",
    headers={"Authorization": f"Bearer {cfg.token}"},
    json={"messages": [{"role": "user", "content": "Hola"}]},
)
print(r.status_code, r.json())
```

Si retorna `200`, está listo.

---

### Paso 6 — Verificar que Genie Code está activo

1. Abre cualquier notebook en el workspace.
2. Verifica que aparece el botón ✨ **Genie Code** en la barra superior derecha del notebook.
3. Si no aparece, ve a **Settings → Feature Preview** → busca **"Databricks Assistant"** → habilítalo.
4. Pide a un participante de prueba que lo abra y confirme que puede generar código con un prompt simple.

---

### Paso 7 — Desplegar la app de instrucciones

La app muestra las instrucciones interactivas del workshop a los participantes.

**Opción A — Desde la CLI de Databricks:**

```bash
# Instala la CLI si no la tienes
pip install databricks-cli

# Sube el código al workspace
databricks workspace import_dir . /Workspace/Users/<tu-usuario>/genie-lafise-workshop --overwrite

# Despliega la app
databricks apps deploy genie-lafise-workshop \
  --source-code-path /Workspace/Users/<tu-usuario>/genie-lafise-workshop
```

**Opción B — Desde la UI:**

1. En la barra lateral, ve a **Apps** → **Create app**.
2. Selecciona **"Custom app"** o **"FastAPI"**.
3. Apunta al path del workspace donde subiste el código.
4. Haz clic en **Deploy**.
5. Una vez desplegada, copia la URL y compártela con los participantes.

---

### Checklist final antes del workshop

Antes de que lleguen los participantes, confirma cada punto:

- [ ] Las 5 tablas existen en `workshop.gold` con datos
- [ ] Los participantes tienen permisos `SELECT` en `workshop.gold`
- [ ] Foundation Model API responde con `200`
- [ ] El botón ✨ Genie Code aparece en notebooks
- [ ] La app de instrucciones está desplegada y accesible
- [ ] Al menos un cluster está corriendo (o en modo serverless) para que los participantes no esperen cold start

---

## Estructura del proyecto

```
genie-code-workshop/
├── app.yaml                    # Configuración Databricks Apps
├── main.py                     # Backend FastAPI
├── requirements.txt            # Dependencias Python
├── generate_workshop_data.py   # Genera las tablas workshop.gold.* (ejecutar una vez)
├── data/
│   └── tracks.json             # Contenido de los 4 tracks (pasos, prompts, FAQs)
└── frontend/
    ├── index.html              # App React (single-page)
    └── img/                    # Logos e íconos
```

---

## Países LAFISE incluidos en los datos

| Código | País | Clientes | Cartera base |
|---|---|---|---|
| NI | Nicaragua | 500 | $180M |
| CR | Costa Rica | 380 | $220M |
| HN | Honduras | 320 | $140M |
| PA | Panamá | 270 | $260M |
| DO | Rep. Dominicana | 250 | $120M |
| SV | El Salvador | 220 | $100M |
| GT | Guatemala | 170 | $90M |
| CO | Colombia | 290 | $195M |

---

## Notas para el facilitador

- Los datos son **100% sintéticos** — no contienen información real de clientes de LAFISE.
- Las tablas incluyen ~382 defectos de calidad intencionados para el track de Governance.
- El track de Data Science requiere ML Runtime en el cluster (para XGBoost y MLflow).
- Los steps que usan Foundation Model API tienen una advertencia visible en la app — ten un notebook de respaldo con el output esperado por si el endpoint no responde.
- El cluster puede compartirse entre todos los participantes durante el workshop, o usar serverless compute si está disponible en el workspace.
