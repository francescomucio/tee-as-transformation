# Missing Incremental Materialization Strategies

## Description

Identify and implement any incremental materialization strategies used in dbt that t4t doesn't currently support.

## Motivation

During dbt import, we need to map dbt's incremental strategies to t4t's incremental strategies. Some strategies may not have direct equivalents in t4t, requiring new implementations.

## Current t4t Incremental Strategies

- `append` - Append new rows
- `merge` - Upsert with merge logic
- `delete+insert` - Delete and re-insert

## dbt Incremental Strategies

- `merge` - Similar to t4t merge
- `append` - Similar to t4t append
- `delete+insert` - Similar to t4t delete+insert
- Others? (to be identified during import implementation)

## Process

1. During dbt import implementation, identify all incremental strategies used
2. Map each strategy to t4t equivalent (or identify as missing)
3. Create separate issues for each missing strategy
4. Implement missing strategies in t4t
5. Update dbt importer to use new strategies

## Related

- Part of dbt importer feature
- Related to incremental materialization system

## Acceptance Criteria

- [ ] All dbt incremental strategies are identified
- [ ] Missing strategies are documented
- [ ] Missing strategies are implemented in t4t
- [ ] dbt importer updated to use all strategies
- [ ] Documentation updated

## Note

This issue will be updated with specific missing strategies once they are identified during dbt importer implementation.

