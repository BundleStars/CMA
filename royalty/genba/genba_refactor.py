# run_genba_local_compare.py
from pathlib import Path
import os
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv
import psycopg2
import re

# ----------------- CONFIG -----------------
EXCEL_PATH = Path("genba_aug.xlsx")  # your Excel
SCHEMA = "royalty"                   # used only for SQL that reads Redshift
TABLE = "genba_v3_202508"            # only used to infer default month window, not queried
load_dotenv()
RH = os.getenv("REDSHIFT_HOST")
RP = int(os.getenv("REDSHIFT_PORT", "5439"))
RD = os.getenv("REDSHIFT_DB")
RU = os.getenv("REDSHIFT_USER")
RPW = os.getenv("REDSHIFT_PASSWORD")

# Excel -> canonical names
COLMAP = {
    "Date of Sale": "date_of_sale",
    "Original Date of Sale": "original_date_of_sale",
    "Date Fulfilled": "date_fulfilled",
    "Transaction GUID": "transaction_guid",
    "CTID": "ctid_1",
    "Publisher Name": "publisher_name",
    "Product Title": "product_title",
    "SKU": "sku",
    "Genba Product ID": "genba_product_id",
    "Country Sold": "country_sold",
    "Activation Qty": "activation_qty",
    "Activation Currency": "activation_currency",
    "SRP Activation Currency": "srp_activation_currency",
    "Promotion %": "promotion_%",
    "WSP Activation Currency": "wsp_activation_currency",
    "Service Charge Activation Currency": "service_charge_activation_currency",
    "Exchange Rate": "exchange_rate",
    "Billing Currency": "billing_currency",
    "WSP Billing Currency": "wsp_billing_currency",
    "WSP VAT": "wsp_vat",
    "Service Charge Billing Currency": "service_charge_billing_currency",
    "Service Charge VAT": "service_charge_vat",
    "Grand Total": "grand_total",
}
# ------------------------------------------

def previous_month_bounds():
    today = date.today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    start = last_prev.replace(day=1)
    end_excl = first_this
    yyyymm = f"{last_prev.year}{last_prev.month:02d}"
    return yyyymm, start, end_excl

def read_excel_normalise(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path)
    missing_src = [src for src in COLMAP.keys() if src not in df.columns]
    if missing_src:
        raise ValueError(f"Excel missing columns: {missing_src}")
    df = df.rename(columns=COLMAP)

    # types
    for c in ["date_of_sale", "original_date_of_sale", "date_fulfilled"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df["activation_qty"] = pd.to_numeric(df["activation_qty"], errors="coerce")
    for c in [
        "srp_activation_currency", "wsp_activation_currency",
        "service_charge_activation_currency", "exchange_rate",
        "wsp_billing_currency", "wsp_vat",
        "service_charge_billing_currency", "service_charge_vat",
        "grand_total"
    ]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def build_genba_cte_from_excel(df: pd.DataFrame) -> pd.DataFrame:
    """Replicates your SQL 'genba' CTE purely in pandas."""
    out = df.copy()

    # iid = split_part(ctid_1,'-',1)
    out["iid"] = out["ctid_1"].astype(str).str.split("-").str[0]

    # numeric safety
    out["activation_qty"] = pd.to_numeric(out["activation_qty"], errors="coerce")
    out["wsp_billing_currency"] = pd.to_numeric(out["wsp_billing_currency"], errors="coerce")
    out["service_charge_billing_currency"] = pd.to_numeric(out["service_charge_billing_currency"], errors="coerce")

    out["original_date_of_sale"] = pd.to_datetime(out["original_date_of_sale"], errors="coerce").dt.date

    # genba_reported_royalty = sum(wsp_billing_currency) + sum(service_charge_billing_currency)
    grp_cols = [
        "ctid_1", "iid", "product_title", "genba_product_id",
        "original_date_of_sale", "country_sold", "activation_currency"
    ]
    agg = out.groupby(grp_cols, dropna=False).agg(
        activation_qty=("activation_qty", "sum"),
        wsp_sum=("wsp_billing_currency", "sum"),
        svc_sum=("service_charge_billing_currency", "sum"),
    ).reset_index()

    agg["genba_reported_royalty"] = (agg["wsp_sum"].fillna(0) + agg["svc_sum"].fillna(0))
    agg = agg.drop(columns=["wsp_sum", "svc_sum"])

    # having sum(activation_qty) > 0
    agg = agg[(agg["activation_qty"].fillna(0) > 0)]

    # rename to match SQL output column names
    agg = agg.rename(columns={
        "activation_currency": "genba_currency"
    })

    return agg

def fetch_fanatical(conn, start_date, end_excl) -> pd.DataFrame:
    sql = f"""
        SELECT iid, product_name
        , a.product_id
        , 1 sales
        , royalty / 100 fanatical_reported_royalty -- this is now genba wsp
        , (revenue_ex_vat_pf*a.royalty_percentage) / 100 f_royalty_calc -- ignore fees
        , (revenue_ex_vat_pf/100) * ((1-a.royalty_percentage)*nvl(genba_service_charge/100,0.125) + a.royalty_percentage) fanatical_assumed_royalty 
        , ((1-a.royalty_percentage)*nvl(genba_service_charge,0.125) + a.royalty_percentage) 
        , a.royalty_percentage
        , genba_service_charge
        , order_date::date
        , status
        , order_id
        , a.currency
        , supplier_name
        , case when c.product_id is not null or b.star_deal then 'Star Deal' else nvl(bundle_name,promo_name) end deal
        , product_discount_self_fund_percent
        , b.discount_percent - self_funded_percent expected_discount
        , vat_rate
        , nvl(case when allows_transaction_fees then transaction_fee/100 end,0) allowable_transaction_fee
        from shop.order_details a
        left join shop.products using(product_id)
        left join shop.suppliers using(supplier_id)
        left join shop.product_discounts b
        on a.product_id = b.product_id 
        AND order_date >= %s
        AND order_date < %s
        left join shop.star_deals c
        on a.product_id = c.product_id
        AND order_date >= %s
        AND order_date < %s
    """
    # params = {
    #     "valid_from": start_date,
    #     "valid_until": end_excl,
    #     "start_date": start_date,
    #     "end_date": end_excl,
    # }


    params = (start_date, end_excl, start_date, end_excl)

    n_placeholders = len(re.findall(r'%s', sql))
    print("placeholders:", n_placeholders, "params:", len(params))

    # Stream in chunks so it doesn't hang on a huge single fetch
    parts = []
    for i, chunk in enumerate(pd.read_sql_query(sql, conn, params=params, chunksize=200_000), 1):
        print(f"  fetched chunk {i} ({len(chunk)} rows)")
        parts.append(chunk)
    df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    print(f"Fetched {len(df)} rows from Redshift for Fanatical CTE")

    # normalize column names expected by downstream merge
    df = df.rename(columns={
        # "product_name": "product_title",
        "currency": "fanatical_currency"
    })
    return df

def compute_raw(genba_df: pd.DataFrame, fan_df: pd.DataFrame) -> pd.DataFrame:
    raw = genba_df.merge(fan_df, on=["iid"], how="left", suffixes=("", "_fan"))
    # carry forward names to match your raw SQL output
    raw = raw.rename(columns={
        "supplier_name": "supplier_name",
        "genba_currency": "genba_currency"  # already named
    })
    # derived metrics
    raw["overcharge"] = raw["genba_reported_royalty"] - raw["fanatical_reported_royalty"].fillna(0)
    raw["overcharge_after_transaction_fee_handling"] = (
        raw["genba_reported_royalty"]
        - raw["allowable_transaction_fee"].fillna(0)
        - raw["fanatical_reported_royalty"].fillna(0)
    )
    denom = raw["fanatical_reported_royalty"].replace({0: pd.NA})
    raw["overcharge_perc"] = ((raw["genba_reported_royalty"] - raw["fanatical_reported_royalty"]) / denom) * 100
    raw["missing_discount"] = 1 - (raw["fanatical_reported_royalty"] / raw["genba_reported_royalty"].replace({0: pd.NA}))
    raw["currency_match"] = (raw["fanatical_currency"] == raw["genba_currency"])
    print("calculated derived fields")

    # optional flags (placeholders if you don't have genba_requests handy)
    raw["rereported_order"] = False
    raw["old_order"] = (pd.to_datetime(raw["original_date_of_sale"]) - pd.to_datetime(raw["order_date"])).dt.days.abs() < 3

    # reorder a few headline columns like your SQL output (optional)
    preferred = [
        "ctid_1","iid","product_title","genba_product_id","product_id","order_id",
        "activation_qty","original_date_of_sale","order_date","status","country_sold",
        "fanatical_currency","genba_currency","deal","supplier_name",
        "genba_reported_royalty","fanatical_reported_royalty",
        "overcharge","overcharge_after_transaction_fee_handling","overcharge_perc",
    ]
    cols = preferred + [c for c in raw.columns if c not in preferred]
    raw = raw[cols]
    return raw

def main():
    if not EXCEL_PATH.exists():
        raise FileNotFoundError(EXCEL_PATH)
    if not all([RH, RD, RU, RPW]):
        raise RuntimeError("Missing REDSHIFT_* environment variables.")

    # month window (previous full month by default)
    yyyymm, start_date, end_excl = previous_month_bounds()
    print(f"Window: {start_date} to (excl) {end_excl} [{yyyymm}]")

    # 1) Excel -> pandas "genba" CTE
    excel_df = read_excel_normalise(EXCEL_PATH)
    genba_df = build_genba_cte_from_excel(excel_df)

    # 2) Redshift -> pandas "fanatical" CTE
    conn = psycopg2.connect(host=RH, port=RP, dbname=RD, user=RU, password=RPW)
    try:
        fan_df = fetch_fanatical(conn, start_date, end_excl)
    finally:
        conn.close()

    # 3) Merge + derived fields (replicate raw SQL)
    raw = compute_raw(genba_df, fan_df)

    # 4) Pivot by Product Title summing overcharge_after_transaction_fee_handling
    pivot_df = (
        raw.pivot_table(index="product_title",
                        values="overcharge_after_transaction_fee_handling",
                        aggfunc="sum", fill_value=0)
           .reset_index()
           .rename(columns={"overcharge_after_transaction_fee_handling": "Total Overcharge"})
    )
    pivot_df = pivot_df.sort_values("Total Overcharge", ascending=False, ignore_index=True)

    # 5) Save locally (or push to Google Sheets if desired)
    pivot_out = f"pivot_{yyyymm}.csv"
    # raw_out = f"raw_{yyyymm}.csv"
    pivot_df.to_csv(pivot_out, index=False)
    # raw.to_csv(raw_out, index=False)
    # print(f"Wrote {pivot_out} and {raw_out}")

if __name__ == "__main__":
    main()
