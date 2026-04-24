### DFL Readme

for the DFL mode, please set the following environment variables in the node env files:
- `AGGREGATION_MODE=decentralized`
- `MIN_PARAMS=1` (or more, depending on your setup)

For example in mnist1.env, mnist2.env, mnist3.env:
# Aggregation mode: "centralized" (default, uses central aggregator) or "decentralized" (node aggregates from peers)
AGGREGATION_MODE=decentralized
# Minimum number of peer submodels to collect before aggregating (DFL mode only)
MIN_PARAMS=3