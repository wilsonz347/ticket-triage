# Design

## Goal

Score every open ticket with the probability that it will not be resolved within 30 days, and present the queue ranked by risk.

## Data

Public Apache JIRA tickets (Kafka, Spark, Hadoop, Flink, Cassandra) created since 2022-01-01, read from the public REST API. See the README for fields.

## Approach

- **Label:** a ticket is positive if it is not resolved within 30 days of creation. Tickets younger than 30 days have no label yet and are excluded from training and evaluation.
- **Validation:** chronological only. Train on earlier tickets, test on later ones. No shuffling across time.
- **Features:** only information available when the ticket was created. Priority, components and text can be edited later, so they are treated with care.
- **Baselines:** historical resolution rate per project, and per project and priority. A model is only useful if it beats these.

## Architecture

| Stage | Schedule | Output |
|---|---|---|
| Collect | Daily | Up-to-date local copy of tickets |
| Prepare | After collect | Features and labels |
| Retrain | Weekly | New model, promoted only if better than the current one |
| Score | After collect | Risk score for every open ticket |
| Dashboard | Always on | Risk-ranked queue and model health |

Scheduled stages run on GitHub Actions. The dashboard is a Streamlit app, deployed automatically when changes are pushed. Tests run on every push.

## Monitoring

- **Pipeline health:** did each run complete, and how many tickets arrived.
- **Data drift:** is incoming data still similar to the training data.
- **Accuracy over time:** compare predictions with outcomes once tickets reach 30 days, so this signal lags by 30 days.
- **Score distribution:** sudden changes in the share of high-risk tickets.

## Decisions

- 30-day horizon and chronological validation, as described above.
- A new model replaces the current one only if it performs better on the newest labelled tickets.

## Open questions

- Can the daily update fetch only tickets changed since the last run, and how does JIRA behave under repeated automated requests?
- How are deleted or moved tickets reflected in the local copy?
- How should later edits to a ticket be stored so that training data stays correct as of the creation date?
- Exact promotion rule for a new model.
- Where the data and model files are stored.

## Milestones

1. Collect
2. Prepare
3. Train and evaluate
4. Score
5. Dashboard
6. CI/CD and scheduling
7. Monitoring
