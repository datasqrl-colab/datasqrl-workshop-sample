# Order Items Enrichment Pipeline

Flattens nested e-commerce orders into individual order items, enriches them
with customer location data and product attributes using temporal joins, applies
data quality checks, and produces daily order processing summaries.

## What It Does

1. **Flatten**: Explodes the `items` array inside each `OrderEvent` into one row
   per order item using `UNNEST` — turning a nested structure into a flat stream.
2. **Enrich with Customer & Product Data**: LEFT temporal joins against the
   `Customers` and `Products` versioned tables to attach attributes *as they were
   at order time*. LEFT joins ensure items without a matching master record are
   retained rather than dropped, so data quality issues become visible.
3. **Data Quality**: Computes `invalid_reason` — the first of these conditions
   that applies: product_id null, seller_id null, customer zip null, product
   category null, or price negative. Null means the item is clean.
4. **Route**: Valid items (null `invalid_reason`) are exported to the enriched
   sink; invalid items are routed to a separate `InvalidOrders` table and sink.
5. **Daily Summary**: A CUMULATE window (1-day total, 10-minute firing interval)
   counts order items and distinct order IDs per calendar day. The intermediate
   firings are deduplicated to one final row per day.

## Architecture

Only Apache Flink is used — no database or API layer needed since the outputs are
JSONL files. The temporal joins are point-in-time consistent: if customer or
product master data changes after an order is placed, the enrichment still
reflects the state at the time of the order.

## Data Sources

From `catalog/ecommerce-sources/`:
- `OrderEvents` — stream of customer orders with nested items array
- `Customers` — CDC stream deduplicated into a versioned customer table
- `Products` — CDC stream deduplicated into a versioned product table

## Output Tables

### EnrichedOrderItems (valid only)
Exported to `data/enriched-order-items/` as JSONL — only items where
`invalid_reason IS NULL`.

Fields: `order_id`, `customer_id`, `order_item_id`, `product_id`, `seller_id`,
`order_purchase_timestamp`, `shipping_limit_date`, `price`, `freight_value`,
`customer_zip`, `customer_city`, `customer_state`, `product_category_name`,
`product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`

### InvalidOrders
Items that failed one or more data quality checks, ordered by
`order_purchase_timestamp DESC`. Exported to `data/invalid-orders/` as JSONL.
Includes `invalid_reason` describing the first failing check.

### InvalidOrdersAfterTime(ts TIMESTAMP)
Table function returning invalid orders at or after the given timestamp —
useful for incremental alerting or downstream error processing.

### OrdersProcessedPerDay
One row per calendar day with `order_item_count` (total items processed) and
`distinct_order_count` (unique orders). Built from a cumulate window that fires
every 10 minutes; deduplicated so each day has only its final accumulated total.

## Running Tests

```bash
/opt/agent/cmd.sh test order-items-enrichment-test-package.json
```
