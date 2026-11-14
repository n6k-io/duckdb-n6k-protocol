# Future work

## Resource limits

Denied operations run on the client. The server only runs work bounded by the rows it ships, which `ratelimits` caps.

```yml
limits:
  # alice can do no wrong
  - when: "$email = 'alice@contoso.com'"
    allow: all
    ratelimits:
      queries: 600/min
  # All other users
  - deny:
      - eval
      - cross_product
      - nested_loop
      - recursive_cte
      - generator
      - many_to_many_join   # join not on a primary_key
      - large_aggregate
      - functions: [regexp_*, repeat, unnest]
    ratelimits:
      queries: 60/min
      rows: 1M/min
      bytes: 100MB/min
```

First matching `when` wins.

Open: how `large_aggregate` is defined.
