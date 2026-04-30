# Order Items Enrichment Pipeline

Flattens nested e-commerce orders into individual order items and enriches them
with customer location data and product attributes using temporal joins.

## What It Does

1. **Flatten**: Explodes the `items` array inside each `OrderEvent` into one row
   per order item using `UNNEST` — turning a nested structure into a flat stream.
2. **Enrich with Customer Data**: Temporal join against the `Customers` versioned
   table to attach the customer's zip code, city, and state *as they were at order time*.
3. **Enrich with Product Data**: Temporal join against the `Products` versioned
   table to attach the product's category, weight, and dimensions *as they were at order time*.
4. **Export**: Writes enriched order items as JSONL to a filesystem sink.

## Architecture

Only Apache Flink is used — no database or API layer needed since the output is
a JSONL file. The temporal joins are point-in-time consistent: if customer or
product master data changes after an order is placed, the enrichment still
reflects the state at the time of the order.

## Data Sources

From `catalog/ecommerce-sources/`:
- `OrderEvents` — stream of customer orders with nested items array
- `Customers` — CDC stream deduplicated into a versioned customer table
- `Products` — CDC stream deduplicated into a versioned product table

## Output

`EnrichedOrderItems` exported to `/tmp/enriched-order-items/` as JSONL with fields:
- Order: `order_id`, `customer_id`, `order_item_id`, `product_id`, `seller_id`, `order_purchase_timestamp`, `shipping_limit_date`, `price`, `freight_value`
- Customer: `customer_zip`, `customer_city`, `customer_state`
- Product: `product_category_name`, `product_weight_g`, `product_length_cm`, `product_height_cm`, `product_width_cm`

## Running Tests

```bash
/opt/agent/cmd.sh test order-items-enrichment-test-package.json
```
