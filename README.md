# Ticket Triage

Predicting which open tickets will take longer than 30 days to resolve, using live public data from Apache JIRA.

An end-to-end ML system: a scheduled data pipeline, time-aware validation, CI/CD and monitoring.

## Problem

Engineering and support teams manage queues of bug reports and requests. Some tickets are resolved within days, others remain open for months, and the difference is hard to see when a ticket is filed. Ticket Triage scores each open ticket with the probability that it will not be resolved within 30 days, so a team can review the highest-risk tickets first.

## Data

| | |
|---|---|
| Source | Public Apache JIRA REST API (`issues.apache.org/jira`), no account or key required |
| Projects | Kafka, Spark, Hadoop, Flink, Cassandra |
| Scope | Tickets created since 2022-01-01 |
| Fields used | Project, issue type, priority, components, reporter, title, description, created and resolved dates |
| Freshness | New tickets are created daily; the pipeline refreshes the data on a schedule |

## Workflow

```mermaid
flowchart LR
  JIRA[("Apache JIRA")]:::source --> A

  subgraph loop["Scheduled pipeline (GitHub Actions)"]
    direction LR
    A["Collect<br/>daily"]:::data --> B["Prepare<br/>features, labels"]:::data --> C["Retrain<br/>weekly"]:::model --> D["Score<br/>open tickets"]:::model
  end

  subgraph cicd["Delivery (CI/CD)"]
    direction LR
    E["Push"]:::eng --> F["Tests"]:::eng --> G["Deploy"]:::eng
  end

  D --> H["Dashboard<br/>risk-ranked queue"]:::user
  G --> H
  M["Monitoring<br/>health, drift, accuracy"]:::watch -.-> H

  classDef source fill:#f1efe8,stroke:#5f5e5a,color:#2c2c2a
  classDef data fill:#e1f5ee,stroke:#0f6e56,color:#04342c
  classDef model fill:#eeedfe,stroke:#534ab7,color:#26215c
  classDef eng fill:#f1efe8,stroke:#5f5e5a,color:#2c2c2a
  classDef user fill:#faece7,stroke:#993c1d,color:#4a1b0c
  classDef watch fill:#faeeda,stroke:#854f0b,color:#412402
  style loop fill:none,stroke:#888780,stroke-dasharray:4 4
  style cicd fill:none,stroke:#888780,stroke-dasharray:4 4
```

- **Collect:** daily pull of new and changed tickets from JIRA.
- **Prepare:** build features and 30-day labels.
- **Retrain:** weekly; a new model replaces the current one only if it performs better.
- **Score:** risk score for every open ticket.
- **Dashboard:** the queue ranked by risk.
- **Monitoring:** pipeline health, data drift and accuracy over time.

## Approach

- Validation is chronological: models train on earlier tickets and are tested on later ones.
- A ticket is labelled only once it is at least 30 days old.
- Features use only information available when the ticket was created.
- Every model is compared against simple baselines, such as the historical resolution rate per project.

## Status

In development. The data pipeline is the first milestone.

## License

[MIT](LICENSE)
