# Data Quality Report — Olist E-Commerce Dataset

**Generated:** 2026-06-02
**Source dataset:** `our-project-93971.Supabase_data`
**DQ dataset:** `our-project-93971.olist_dev_data_quality`
**dbt run:** 24/24 models OK · 53/53 tests PASS

---

## Summary

| Table | Total rows | Flagged rows | Flag rate | Status |
|---|---:|---:|---:|---|
| orders | 99,441 | 0 | 0.0% | ✅ Clean |
| customers | 99,441 | 0 | 0.0% | ✅ Clean |
| order_items | 112,650 | 0 | 0.0% | ✅ Clean |
| order_payments | 103,886 | 9 | 0.01% | ⚠️ Minor issues |
| order_reviews | 99,224 | 0 | 0.0% | ✅ Clean |
| products | 32,951 | 611 | 1.86% | ⚠️ Issues found |
| sellers | 3,095 | 0 | 0.0% | ✅ Clean |
| geolocation | 1,000,163 | 42 | 0.0% | ⚠️ Minor issues |
| category_name_translation | 71 | 0 | 0.0% | ✅ Clean |

**6 of 9 tables are fully clean. 3 tables have data quality issues totalling 662 flagged rows.**

---

## Issue Details

### order_payments — 9 flagged rows

| Issue | Count |
|---|---:|
| `zero_or_negative_payment_value` | 6 |
| `invalid_payment_type, zero_or_negative_payment_value` | 3 |

**What this means:** 9 payment records have a payment value of zero or less, and 3 of those also have an unrecognised payment type. Valid payment types are: `credit_card`, `boleto`, `voucher`, `debit_card`.

**Impact on star schema:** These 9 records are excluded from `fact_orders` total payment calculations.

---

### products — 611 flagged rows

| Issue | Count |
|---|---:|
| `null_category_name` | 609 |
| `null_dimensions` | 1 |
| `null_category_name, null_dimensions` | 1 |

**What this means:** 610 products have no Portuguese category name in the raw data (1.85% of the product catalogue). This means no English translation can be looked up either. 2 products also have missing dimension data (length/height/width).

**Impact on star schema:** These 611 products are excluded from `dim_products`. Their `category_name_english` would show as `unknown` but to keep the star schema clean they are excluded entirely.

---

### geolocation — 42 flagged rows

| Issue | Count |
|---|---:|
| `out_of_range_coordinates` | 42 |

**What this means:** 42 geolocation records have latitude/longitude coordinates outside Brazil's geographic bounds (lat: −33.75° to 5.27°, lng: −73.99° to −34.79°). These are likely data entry errors.

**Impact on star schema:** Geolocation is not yet joined into the star schema. These 42 rows are captured for reference.

---

## Checks Defined Per Table

| Table | Checks |
|---|---|
| `dq_orders` | null_customer_id, null_order_status, invalid_order_status, delivered_before_purchased |
| `dq_customers` | null_customer_id, null_customer_unique_id, null_zip_code |
| `dq_order_items` | null_order_id, null_product_id, null_seller_id, negative_price, negative_freight |
| `dq_order_payments` | null_order_id, zero_or_negative_payment_value, invalid_payment_type |
| `dq_order_reviews` | null_review_id, null_order_id, invalid_review_score |
| `dq_products` | null_product_id, null_category_name, null_dimensions, negative_weight |
| `dq_sellers` | null_seller_id, null_zip_code |
| `dq_geolocation` | null_lat_lng, out_of_range_coordinates |
| `dq_category_name_translation` | null_category_name, null_english_name |

---

## How to Query Issues

Each DQ table in BigQuery contains only flagged rows, with:
- One boolean column per check (e.g. `null_category_name = TRUE`)
- An `issues` column summarising all problems on that row as a comma-separated string

**Example — view all flagged products:**
```sql
SELECT product_id, issues
FROM `our-project-93971.olist_dev_data_quality.dq_products`
ORDER BY issues;
```

**Example — count issues by type across all payments:**
```sql
SELECT issues, COUNT(*) AS cnt
FROM `our-project-93971.olist_dev_data_quality.dq_order_payments`
GROUP BY issues
ORDER BY cnt DESC;
```
