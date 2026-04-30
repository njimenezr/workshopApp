# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Data Generator — LAFISE Genie Code Workshop
# MAGIC
# MAGIC Genera 5 tablas sintéticas de datos bancarios en `workshop.gold`.
# MAGIC Incluye defectos de calidad intencionados para el track de Governance.
# MAGIC
# MAGIC **Idempotente** — seguro de re-ejecutar. Usa `CREATE OR REPLACE TABLE`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

CATALOG = "workshop"
SCHEMA = "gold"

# Presencia de LAFISE por país
COUNTRIES = {
    "NI": {
        "name": "Nicaragua",
        "customers": 500,
        "branches": 28,
        "cities": ["Managua", "León", "Granada", "Masaya", "Estelí", "Matagalpa", "Chinandega", "Rivas"],
        "lat_range": (11.0, 15.0),
        "lon_range": (-87.5, -83.0),
        "base_portfolio": 180_000_000,   # USD
    },
    "CR": {
        "name": "Costa Rica",
        "customers": 380,
        "branches": 22,
        "cities": ["San José", "Heredia", "Alajuela", "Cartago", "Liberia", "Pérez Zeledón"],
        "lat_range": (8.0, 11.2),
        "lon_range": (-85.9, -82.5),
        "base_portfolio": 220_000_000,
    },
    "HN": {
        "name": "Honduras",
        "customers": 320,
        "branches": 18,
        "cities": ["Tegucigalpa", "San Pedro Sula", "Choloma", "La Ceiba", "El Progreso"],
        "lat_range": (13.0, 16.5),
        "lon_range": (-89.2, -83.2),
        "base_portfolio": 140_000_000,
    },
    "PA": {
        "name": "Panamá",
        "customers": 270,
        "branches": 15,
        "cities": ["Ciudad de Panamá", "David", "Santiago", "Colón", "La Chorrera"],
        "lat_range": (7.2, 9.6),
        "lon_range": (-83.0, -77.2),
        "base_portfolio": 260_000_000,
    },
    "DO": {
        "name": "Rep. Dominicana",
        "customers": 250,
        "branches": 14,
        "cities": ["Santo Domingo", "Santiago", "La Romana", "San Pedro de Macorís", "Puerto Plata"],
        "lat_range": (17.5, 19.9),
        "lon_range": (-72.0, -68.3),
        "base_portfolio": 120_000_000,
    },
    "SV": {
        "name": "El Salvador",
        "customers": 220,
        "branches": 13,
        "cities": ["San Salvador", "Santa Ana", "San Miguel", "Soyapango", "Nueva San Salvador"],
        "lat_range": (13.1, 14.4),
        "lon_range": (-90.1, -87.7),
        "base_portfolio": 100_000_000,
    },
    "GT": {
        "name": "Guatemala",
        "customers": 170,
        "branches": 10,
        "cities": ["Ciudad de Guatemala", "Quetzaltenango", "Villa Nueva", "Escuintla", "Cobán"],
        "lat_range": (13.7, 17.8),
        "lon_range": (-92.2, -88.2),
        "base_portfolio": 90_000_000,
    },
    "CO": {
        "name": "Colombia",
        "customers": 290,
        "branches": 16,
        "cities": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga", "Pereira"],
        "lat_range": (-4.2, 12.5),
        "lon_range": (-77.0, -66.9),
        "base_portfolio": 195_000_000,
    },
}

SEGMENTS = ["Retail", "PYME", "Corporativo", "Premium"]
SEGMENT_WEIGHTS = [0.55, 0.25, 0.10, 0.10]
RISK_PROFILES = ["A", "B", "C", "D", "E"]
RISK_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]
KYC_STATUSES = ["Vigente", "Vencido", "Pendiente"]
KYC_WEIGHTS = [0.80, 0.12, 0.08]
PRODUCT_TYPES = ["Consumo", "Hipoteca", "Vehiculo", "Comercial", "Tarjeta"]
PRODUCT_WEIGHTS = [0.38, 0.22, 0.18, 0.14, 0.08]
TRANSACTION_TYPES = ["Débito", "Crédito", "Transferencia", "Pago"]
CHANNELS = ["Sucursal", "App", "ATM", "Web", "Corresponsal"]
CHANNEL_WEIGHTS = [0.25, 0.35, 0.18, 0.15, 0.07]
PRODUCTS_TX = ["Cuenta_Ahorros", "Cuenta_Corriente", "Tarjeta_Credito", "Deposito_Plazo"]
BRANCH_TYPES = ["Sucursal", "Agencia", "Corresponsal", "Digital"]
DPD_BUCKETS = ["Al_Dia", "1-30", "31-60", "61-90", "Mayor_90"]

total_customers = sum(c["customers"] for c in COUNTRIES.values())
total_branches = sum(c["branches"] for c in COUNTRIES.values())
print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Total clientes: {total_customers:,} | Total sucursales: {total_branches}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Catálogo y Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ {CATALOG}.{SCHEMA} listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 1: dim_clientes

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import date, timedelta

np.random.seed(42)

customer_rows = []
customer_id_counter = 0

for cc, info in COUNTRIES.items():
    for i in range(info["customers"]):
        customer_id_counter += 1
        cid = f"LAFISE-{cc}-CLI-{customer_id_counter:06d}"
        city = np.random.choice(info["cities"])
        segment = np.random.choice(SEGMENTS, p=SEGMENT_WEIGHTS)
        risk = np.random.choice(RISK_PROFILES, p=RISK_WEIGHTS)
        kyc = np.random.choice(KYC_STATUSES, p=KYC_WEIGHTS)

        # Credit score correlated with risk profile
        score_ranges = {"A": (720, 850), "B": (620, 720), "C": (520, 620), "D": (420, 520), "E": (300, 420)}
        credit_score = int(np.random.uniform(*score_ranges[risk]))

        acquisition_date = date(2010, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 14)))

        # Relationship managers per country
        rm_names = [
            f"Ana García ({cc})", f"Carlos Méndez ({cc})", f"María López ({cc})",
            f"Jorge Solís ({cc})", f"Patricia Vega ({cc})", f"Roberto Acuña ({cc})"
        ]
        rm = np.random.choice(rm_names)

        customer_rows.append({
            "customer_id": cid,
            "customer_name": f"Cliente {cc} {i+1:04d}",
            "segment": segment,
            "country_code": cc,
            "country_name": info["name"],
            "city": city,
            "credit_score": credit_score,
            "risk_profile": risk,
            "kyc_status": kyc,
            "acquisition_date": acquisition_date,
            "relationship_manager": rm,
        })

df_customers = pd.DataFrame(customer_rows)

# ── Inyectar defectos DQ (contexto bancario) ──
# 10 filas: NULL country_code
null_cc_idx = np.random.choice(len(df_customers), 10, replace=False)
df_customers.loc[null_cc_idx, "country_code"] = None

# 6 filas: fecha de adquisición futura
future_idx = np.random.choice(len(df_customers), 6, replace=False)
df_customers.loc[future_idx, "acquisition_date"] = date(2027, 3, 15)

# 5 filas: customer_id duplicado
dup_idx = np.random.choice(len(df_customers), 5, replace=False)
for idx in dup_idx:
    source_idx = np.random.choice([i for i in range(len(df_customers)) if i != idx])
    df_customers.loc[idx, "customer_id"] = df_customers.loc[source_idx, "customer_id"]

# 15 filas: segment en minúsculas (inconsistencia de capitalización)
case_idx = np.random.choice(len(df_customers), 15, replace=False)
df_customers.loc[case_idx, "segment"] = df_customers.loc[case_idx, "segment"].str.lower()

# 18 filas: credit_score fuera de rango (<300 o >850)
score_idx = np.random.choice(len(df_customers), 18, replace=False)
df_customers.loc[score_idx[:9], "credit_score"] = 150   # imposible — por debajo del mínimo
df_customers.loc[score_idx[9:], "credit_score"] = 950   # imposible — por encima del máximo

print(f"dim_clientes: {len(df_customers)} filas")
print(f"  Defectos DQ: 10 null country, 6 fechas futuras, 5 IDs dup, 15 segment minúsculas, 18 scores inválidos")

sdf = spark.createDataFrame(df_customers)
sdf.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_clientes")
print(f"✅ {CATALOG}.{SCHEMA}.dim_clientes escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 2: dim_sucursales

# COMMAND ----------

branch_rows = []
branch_counter = 0
valid_branch_ids = []

for cc, info in COUNTRIES.items():
    for i in range(info["branches"]):
        branch_counter += 1
        bid = f"SUC-{cc}-{branch_counter:04d}"
        city = np.random.choice(info["cities"])
        btype = np.random.choice(BRANCH_TYPES, p=[0.40, 0.30, 0.20, 0.10])
        lat = np.random.uniform(*info["lat_range"])
        lon = np.random.uniform(*info["lon_range"])
        region = np.random.choice(["Norte", "Centro", "Sur", "Capital"])
        valid_branch_ids.append(bid)

        branch_rows.append({
            "branch_id": bid,
            "branch_name": f"LAFISE {city} #{i+1}",
            "city": city,
            "country_code": cc,
            "country_name": info["name"],
            "branch_type": btype,
            "region": region,
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
        })

df_branches = pd.DataFrame(branch_rows)

# ── Inyectar defectos DQ ──
# 8 filas: NULL country_code
null_cc_b = np.random.choice(len(df_branches), 8, replace=False)
df_branches.loc[null_cc_b, "country_code"] = None

# 3 filas: branch_id duplicado
dup_b = np.random.choice(len(df_branches), 3, replace=False)
for idx in dup_b:
    src = np.random.choice([i for i in range(len(df_branches)) if i != idx])
    df_branches.loc[idx, "branch_id"] = df_branches.loc[src, "branch_id"]

# 10 filas: branch_type en minúsculas
case_b = np.random.choice(len(df_branches), 10, replace=False)
df_branches.loc[case_b, "branch_type"] = df_branches.loc[case_b, "branch_type"].str.lower()

print(f"dim_sucursales: {len(df_branches)} filas")
print(f"  Defectos DQ: 8 null country, 3 IDs dup, 10 branch_type minúsculas")

sdf_b = spark.createDataFrame(df_branches)
sdf_b.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_sucursales")
print(f"✅ {CATALOG}.{SCHEMA}.dim_sucursales escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 3: fact_transacciones

# COMMAND ----------

valid_customer_ids = df_customers["customer_id"].dropna().unique().tolist()
date_range = pd.date_range("2025-01-01", "2026-04-30", freq="D")
tx_rows = []

# Generar transacciones para los primeros 600 clientes (muestra representativa)
sample_customers = valid_customer_ids[:600]

for cid in sample_customers:
    # Obtener datos del cliente
    cust = df_customers[df_customers["customer_id"] == cid].iloc[0]
    cc = cust["country_code"] if pd.notna(cust["country_code"]) else "NI"
    segment = cust["segment"]

    # Frecuencia según segmento
    freq_per_month = {"Retail": 8, "PYME": 15, "Corporativo": 25, "Premium": 12}.get(segment, 8)

    # Monto base según segmento y país
    country_mult = {"NI": 1.0, "CR": 1.8, "HN": 0.9, "PA": 2.2, "DO": 1.0, "SV": 0.95, "GT": 0.85, "CO": 1.3}
    base_amount = {"Retail": 350, "PYME": 5000, "Corporativo": 50000, "Premium": 8000}.get(segment, 350)
    base_amount *= country_mult.get(cc, 1.0)

    branch_pool = [b for b in valid_branch_ids if cc in b]
    if not branch_pool:
        branch_pool = valid_branch_ids[:5]

    for d in date_range:
        # Probabilidad diaria de transacción
        if np.random.random() < freq_per_month / 30:
            n_tx = np.random.randint(1, 4)
            for _ in range(n_tx):
                amount = max(1.0, np.random.lognormal(np.log(base_amount), 0.6))
                tx_type = np.random.choice(TRANSACTION_TYPES)
                channel = np.random.choice(CHANNELS, p=CHANNEL_WEIGHTS)
                product = np.random.choice(PRODUCTS_TX)
                status = np.random.choice(["Aprobada", "Rechazada", "Pendiente"], p=[0.88, 0.08, 0.04])
                branch = np.random.choice(branch_pool)

                tx_rows.append({
                    "transaction_id": f"TX-{cc}-{len(tx_rows)+1:08d}",
                    "customer_id": cid,
                    "branch_id": branch,
                    "transaction_date": d.date(),
                    "amount": round(amount, 2),
                    "transaction_type": tx_type,
                    "channel": channel,
                    "product": product,
                    "currency": "USD",
                    "status": status,
                })

df_tx = pd.DataFrame(tx_rows)

# ── Inyectar defectos DQ ──
# 150 filas: amount = 0 con status 'Aprobada'
zero_amt_idx = np.random.choice(len(df_tx), 150, replace=False)
df_tx.loc[zero_amt_idx, "amount"] = 0.0
df_tx.loc[zero_amt_idx, "status"] = "Aprobada"

# 30 filas: NULL transaction_date
null_date_idx = np.random.choice(len(df_tx), 30, replace=False)
df_tx.loc[null_date_idx, "transaction_date"] = None

# 60 filas: customer_id huérfano (no existe en dim_clientes — integridad referencial rota)
orphan_cids = [f"LAFISE-XX-CLI-{900000+i:06d}" for i in range(60)]
orphan_idx = np.random.choice(len(df_tx), 60, replace=False)
for i, idx in enumerate(orphan_idx):
    df_tx.loc[idx, "customer_id"] = orphan_cids[i]

# 25 filas: amount negativo en transacciones no-reversión
neg_idx = np.random.choice(len(df_tx), 25, replace=False)
df_tx.loc[neg_idx, "amount"] = -abs(df_tx.loc[neg_idx, "amount"])

print(f"fact_transacciones: {len(df_tx):,} filas")
print(f"  Defectos DQ: 150 monto cero aprobado, 30 null fecha, 60 clientes huérfanos, 25 montos negativos")

sdf_tx = spark.createDataFrame(df_tx)
sdf_tx.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_transacciones")
print(f"✅ {CATALOG}.{SCHEMA}.fact_transacciones escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 4: fact_cartera_creditos

# COMMAND ----------

loan_rows = []
loan_counter = 0

for cc, info in COUNTRIES.items():
    country_customers = df_customers[df_customers["country_code"] == cc]["customer_id"].tolist()
    base_port = info["base_portfolio"]
    n_loans = int(base_port / 25_000)   # ~25K USD promedio por crédito

    for i in range(n_loans):
        loan_counter += 1
        product = np.random.choice(PRODUCT_TYPES, p=PRODUCT_WEIGHTS)
        loan_id = f"LAFISE-{cc}-{product[:3].upper()}-{loan_counter:06d}"

        cid = np.random.choice(country_customers) if country_customers else f"LAFISE-{cc}-CLI-000001"

        disb_date = date(2020, 1, 1) + timedelta(days=int(np.random.uniform(0, 365 * 5)))

        # Plazo según producto
        term_months = {
            "Consumo": np.random.choice([12, 24, 36, 48, 60]),
            "Hipoteca": np.random.choice([120, 180, 240, 300]),
            "Vehiculo": np.random.choice([24, 36, 48, 60]),
            "Comercial": np.random.choice([12, 24, 36]),
            "Tarjeta": 12,
        }[product]

        maturity_date = disb_date + timedelta(days=term_months * 30)

        # Monto original según producto y país
        amount_ranges = {
            "Consumo": (2000, 25000),
            "Hipoteca": (40000, 250000),
            "Vehiculo": (8000, 45000),
            "Comercial": (10000, 500000),
            "Tarjeta": (500, 10000),
        }
        original_amount = round(np.random.uniform(*amount_ranges[product]), 2)

        # Saldo pendiente (entre 0 y original)
        elapsed_pct = min(1.0, (date(2026, 4, 30) - disb_date).days / (term_months * 30))
        balance_pct = max(0, 1 - elapsed_pct * np.random.uniform(0.8, 1.2))
        outstanding = round(original_amount * balance_pct, 2)
        monthly_pmt = round(original_amount / term_months * np.random.uniform(1.0, 1.05), 2)

        # Mora — distribución realista por perfil de riesgo
        cust_data = df_customers[df_customers["customer_id"] == cid]
        if len(cust_data) > 0:
            risk = cust_data.iloc[0]["risk_profile"]
        else:
            risk = "B"

        dpd_probs = {
            "A": [0.90, 0.06, 0.025, 0.010, 0.005],
            "B": [0.82, 0.10, 0.045, 0.025, 0.010],
            "C": [0.65, 0.16, 0.090, 0.065, 0.035],
            "D": [0.42, 0.22, 0.160, 0.120, 0.080],
            "E": [0.20, 0.18, 0.160, 0.180, 0.280],
        }
        dpd_bucket = np.random.choice(DPD_BUCKETS, p=dpd_probs.get(risk, dpd_probs["B"]))

        dpd_ranges = {"Al_Dia": (0, 0), "1-30": (1, 30), "31-60": (31, 60), "61-90": (61, 90), "Mayor_90": (91, 730)}
        days_past_due = int(np.random.uniform(*dpd_ranges[dpd_bucket]))

        status_map = {"Al_Dia": "Vigente", "1-30": "Vigente", "31-60": "Vencido", "61-90": "Vencido", "Mayor_90": "Castigado"}
        status = status_map[dpd_bucket]

        # Tasa de interés según producto y país (tasas centroamericanas)
        rate_ranges = {
            "Consumo": (0.14, 0.32),
            "Hipoteca": (0.08, 0.15),
            "Vehiculo": (0.10, 0.20),
            "Comercial": (0.10, 0.25),
            "Tarjeta": (0.24, 0.48),
        }
        interest_rate = round(np.random.uniform(*rate_ranges[product]), 4)

        loan_rows.append({
            "loan_id": loan_id,
            "customer_id": cid,
            "product_type": product,
            "disbursement_date": disb_date,
            "maturity_date": maturity_date,
            "original_amount": original_amount,
            "outstanding_balance": outstanding,
            "monthly_payment": monthly_pmt,
            "days_past_due": days_past_due,
            "dpd_bucket": dpd_bucket,
            "status": status,
            "interest_rate": interest_rate,
            "country_code": cc,
        })

df_loans = pd.DataFrame(loan_rows)

# ── Inyectar defectos DQ ──
# 8 filas: days_past_due negativo (imposible)
neg_dpd_idx = np.random.choice(len(df_loans), 8, replace=False)
df_loans.loc[neg_dpd_idx, "days_past_due"] = -np.random.randint(1, 30, 8)

# 4 filas: maturity_date antes de disbursement_date
bad_maturity_idx = np.random.choice(len(df_loans), 4, replace=False)
df_loans.loc[bad_maturity_idx, "maturity_date"] = df_loans.loc[bad_maturity_idx, "disbursement_date"] - timedelta(days=180)

# 10 filas: outstanding_balance > original_amount * 1.2 (incapitalizaciones mal registradas)
over_balance_idx = np.random.choice(len(df_loans), 10, replace=False)
df_loans.loc[over_balance_idx, "outstanding_balance"] = df_loans.loc[over_balance_idx, "original_amount"] * np.random.uniform(1.25, 1.80, 10)

# 5 filas: NULL interest_rate
null_rate_idx = np.random.choice(len(df_loans), 5, replace=False)
df_loans.loc[null_rate_idx, "interest_rate"] = None

# 12 filas: dpd_bucket inconsistente con days_past_due
# (e.g., days_past_due=45 pero bucket="Al_Dia")
inconsistent_idx = np.random.choice(len(df_loans), 12, replace=False)
for idx in inconsistent_idx:
    actual_dpd = df_loans.loc[idx, "days_past_due"]
    # Asignar bucket incorrecto
    correct_bucket = df_loans.loc[idx, "dpd_bucket"]
    wrong_options = [b for b in DPD_BUCKETS if b != correct_bucket]
    df_loans.loc[idx, "dpd_bucket"] = np.random.choice(wrong_options)

print(f"fact_cartera_creditos: {len(df_loans):,} filas")
print(f"  Defectos DQ: 8 DPD negativos, 4 madurez < desembolso, 10 saldo > original, 5 null tasa, 12 bucket inconsistente")

sdf_loans = spark.createDataFrame(df_loans)
sdf_loans.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_cartera_creditos")
print(f"✅ {CATALOG}.{SCHEMA}.fact_cartera_creditos escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tabla 5: fact_kpis_diarios

# COMMAND ----------

kpi_rows = []
kpi_date_range = pd.date_range("2024-01-01", "2026-04-30", freq="D")

for cc, info in COUNTRIES.items():
    base_port = info["base_portfolio"]

    for d in kpi_date_range:
        # Crecimiento anual ~4%
        years_from_start = (d - pd.Timestamp("2024-01-01")).days / 365
        growth = 1 + 0.04 * years_from_start

        # Estacionalidad mensual
        month_mult = {1: 0.95, 2: 0.90, 3: 0.97, 4: 1.00, 5: 1.02, 6: 1.00,
                      7: 1.03, 8: 1.01, 9: 0.98, 10: 1.00, 11: 1.04, 12: 1.08}[d.month]

        noise = np.random.normal(1.0, 0.03)
        total_port = round(base_port * growth * month_mult * noise, 2)

        # NPL ratio (tasa de mora) — varía por país y tiempo
        npl_base = {"NI": 0.042, "CR": 0.028, "HN": 0.051, "PA": 0.022, "DO": 0.055,
                    "SV": 0.048, "GT": 0.062, "CO": 0.035}.get(cc, 0.04)
        npl_ratio = round(npl_base + np.random.normal(0, 0.003), 4)
        npl_ratio = max(0.005, npl_ratio)

        # Desembolsos del mes
        new_disb = round(total_port * np.random.uniform(0.008, 0.025) * month_mult, 2)

        # Cobros / recuperaciones
        collections = round(total_port * np.random.uniform(0.005, 0.015), 2)

        # Provisiones
        provision_exp = round(total_port * npl_ratio * np.random.uniform(0.15, 0.30), 2)

        active_cust = int(info["customers"] * np.random.uniform(0.75, 0.95))
        new_cust = int(np.random.uniform(0, 8))

        # YoY growth disponible solo desde 2025
        yoy_growth = None
        if d.year >= 2025:
            yoy_growth = round(np.random.normal(0.04, 0.02), 4)

        kpi_rows.append({
            "kpi_date": d.date(),
            "country_code": cc,
            "country_name": info["name"],
            "total_portfolio": total_port,
            "npl_ratio": npl_ratio,
            "new_disbursements": new_disb,
            "collections": collections,
            "provision_expense": provision_exp,
            "active_customers": active_cust,
            "new_customers": new_cust,
            "yoy_portfolio_growth": yoy_growth,
        })

df_kpis = pd.DataFrame(kpi_rows)

# ── Inyectar defectos DQ ──
# 8 filas: npl_ratio > 1.0 (imposible — mayor que la cartera total)
invalid_npl_idx = np.random.choice(len(df_kpis), 8, replace=False)
df_kpis.loc[invalid_npl_idx, "npl_ratio"] = np.random.uniform(1.5, 3.0, 8)

# 5 filas: total_portfolio = 0 con new_disbursements > 0
zero_port_idx = np.random.choice(len(df_kpis), 5, replace=False)
df_kpis.loc[zero_port_idx, "total_portfolio"] = 0.0

# 3 filas: yoy_portfolio_growth = 999.99 (outlier centinela)
sentinel_idx = df_kpis[df_kpis["yoy_portfolio_growth"].notna()].sample(3).index
df_kpis.loc[sentinel_idx, "yoy_portfolio_growth"] = 999.99

# 6 filas: country_code = "DESCONOCIDO"
unknown_idx = np.random.choice(len(df_kpis), 6, replace=False)
df_kpis.loc[unknown_idx, "country_code"] = "DESCONOCIDO"

print(f"fact_kpis_diarios: {len(df_kpis):,} filas")
print(f"  Defectos DQ: 8 NPL>1.0, 5 portfolio=0 con desembolsos, 3 centinelas, 6 país DESCONOCIDO")

sdf_kpis = spark.createDataFrame(df_kpis)
sdf_kpis.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_kpis_diarios")
print(f"✅ {CATALOG}.{SCHEMA}.fact_kpis_diarios escrita")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen de Validación

# COMMAND ----------

print("=" * 65)
print("GENERACIÓN DE DATOS WORKSHOP LAFISE — RESUMEN")
print("=" * 65)

tables = [
    "dim_clientes", "dim_sucursales", "fact_transacciones",
    "fact_cartera_creditos", "fact_kpis_diarios"
]
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.{t}").collect()[0]["cnt"]
    print(f"  {CATALOG}.{SCHEMA}.{t}: {count:,} filas")

print()
print("Defectos DQ inyectados:")
print("  dim_clientes:         10 null country, 6 fechas futuras, 5 IDs dup, 15 segment minúsculas, 18 scores inválidos")
print("  dim_sucursales:       8 null country, 3 IDs dup, 10 branch_type minúsculas")
print("  fact_transacciones:   150 monto cero, 30 null fecha, 60 clientes huérfanos, 25 montos negativos")
print("  fact_cartera_creditos: 8 DPD negativos, 4 madurez<desembolso, 10 saldo>original, 5 null tasa, 12 bucket inconsistente")
print("  fact_kpis_diarios:    8 NPL>1.0, 5 portfolio=0, 3 centinelas, 6 país DESCONOCIDO")
print()
print(f"Total defectos: ~382")
print()
print("Países LAFISE incluidos:")
for cc, info in COUNTRIES.items():
    print(f"  {cc}: {info['name']} — {info['customers']} clientes, {info['branches']} sucursales, ${info['base_portfolio']/1e6:.0f}M cartera")
print("=" * 65)
print("✅ Generación completa. Workshop listo.")
