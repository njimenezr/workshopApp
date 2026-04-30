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

## Preparación del ambiente

### Paso 1 — Crear los datos del workshop

Sube `generate_workshop_data.py` como notebook a Databricks y ejecútalo **una sola vez** sobre cualquier cluster con Unity Catalog habilitado. Crea las siguientes tablas en `workshop.gold`:

| Tabla | Descripción |
|---|---|
| `dim_clientes` | Maestro de clientes (2,400 registros, 8 países) |
| `dim_sucursales` | Sucursales y agencias de LAFISE |
| `fact_transacciones` | Transacciones financieras diarias |
| `fact_cartera_creditos` | Cartera de créditos con DPD buckets |
| `fact_kpis_diarios` | KPIs consolidados por país (NPL, desembolsos, provisiones) |

El notebook es idempotente — se puede re-ejecutar si algo falla.

### Paso 2 — Permisos en Unity Catalog

Ejecuta los siguientes comandos SQL como administrador del workspace:

```sql
GRANT USE CATALOG ON CATALOG workshop TO `workshop_users`;
GRANT USE SCHEMA ON SCHEMA workshop.gold TO `workshop_users`;
GRANT SELECT ON ALL TABLES IN SCHEMA workshop.gold TO `workshop_users`;
```

> Reemplaza `workshop_users` por el grupo o usuarios reales del workspace de LAFISE.

### Paso 3 — Habilitar Foundation Model API

Requerido para los Steps que usan FMAPI (Knowledge Assistant, AI Functions, alertas SARLAFT).

**Settings → Compute → Foundation Model APIs → Enable**

Modelo a usar en los prompts: `databricks-claude-sonnet-4`

### Paso 4 — Verificar Genie Code

Confirma que los participantes ven el botón ✨ en los notebooks de Databricks. Si no aparece, habilítalo en:

**Settings → Feature Preview → Databricks Assistant → Enable**

### Paso 5 — Desplegar la app de instrucciones

La app sirve las instrucciones interactivas del workshop (esta repo).

```bash
# Desde el directorio del proyecto
databricks apps deploy genie-lafise-workshop \
  --source-code-path /Workspace/Users/<tu-usuario>/genie-code-workshop
```

O usa la UI de Databricks Apps y apunta al path donde subiste el código.

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

| Código | País | Cartera base |
|---|---|---|
| NI | Nicaragua | $180M |
| CR | Costa Rica | $220M |
| HN | Honduras | $140M |
| PA | Panamá | $260M |
| DO | Rep. Dominicana | $120M |
| SV | El Salvador | $100M |
| GT | Guatemala | $90M |
| CO | Colombia | $195M |

---

## Notas para el facilitador

- Los datos son **100% sintéticos** — no contienen información real de clientes de LAFISE.
- Las tablas incluyen ~382 defectos de calidad intencionados para el track de Governance.
- El track de Data Science requiere un cluster con ML Runtime (para XGBoost y MLflow).
- Los steps que usan Foundation Model API tienen una nota de advertencia — ten un notebook de respaldo con el output esperado.
