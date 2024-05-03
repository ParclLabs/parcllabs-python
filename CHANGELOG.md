### v0.1.14
- Update `portfolio_metrics` endpoint name from `new_listings_for_sale_rolling_counts` to `sf_new_listings_for_sale_rolling_counts` to reflect the new endpoint name.


### v0.1.13
- bug fix: valid portfolio parameter values.

Valid portfolio size parameter values include: 
- PORTFOLIO_2_TO_9
- PORTFOLIO_10_TO_99
- PORTFOLIO_100_TO_999
- PORTFOLIO_1000_PLUS
- ALL_PORTFOLIOS

### v0.1.12
- bug fix: dataframe index's on `retrieve_many` methods
- Update readme examples

### v0.1.11

- Update `search` to accomodate `sort_order`, `sort_by`
- `sort_by` valid values include `TOTAL_POPULATION`, `MEDIAN_INCOME`, `CASE_SHILLER_20_MARKET`, `CASE_SHILLER_10_MARKET`, `PRICEFEED_MARKET`, `PARCL_EXCHANGE_MARKET`
- include endpoint `/v1/portfolio_metrics/{parcl_id}/new_listings_for_sale_rolling_counts`
- include endpoint `/v1/rental_market_metrics/{parcl_id}/new_listings_for_rent_rolling_counts`
