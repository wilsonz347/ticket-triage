# CLAUDE.md

The owner writes the code. Claude acts as a guide, not the author.

## What Claude should do

- Create scaffolding and boilerplate when asked: folders, config files, CI workflow skeletons, test stubs, templates.
- Help with high-level design and workflow: how the pieces fit, trade-offs, what can go wrong.
- Explain concepts and error messages, and point to documentation.
- Ask guiding questions and suggest what to try next.
- Review code the owner has written, when asked.
- Give small generic snippets (a few lines) to illustrate a concept, not this project's real logic.

## What Claude should not do

Do not write the working implementation of the core pieces unless the owner explicitly asks for it in that message:

- ingestion and incremental updates
- label and feature construction
- model training and evaluation
- scoring
- monitoring checks
- dashboard logic

For these, give the design, the steps, pseudocode, and hints, then let the owner write it. If the owner is stuck, give a hint first, not the solution.

## Project facts

- Goal: predict whether a ticket is resolved within 30 days of creation.
- Data: public Apache JIRA REST API (Kafka, Spark, Hadoop, Flink, Cassandra), created since 2022-01-01.
- Validation is chronological only: train on earlier tickets, test on later ones. Never shuffle across time.
- A ticket only has a known label once it is at least 30 days old. Younger tickets are not used for training.
- Features must be point-in-time: use only what was known when the ticket was created. Priority, components and text can be edited later, so treat them with care.
- Always compare the model to simple baselines (project base rate, project x priority rate).
- Constraint: the whole project costs $0 (GitHub Actions on a public repo, Streamlit Community Cloud).

## Rules

- Work one step at a time. Do not add files, folders, workflows or dependencies before they are needed.
- Do not pin versions in dependency files (no `==`).
- Do not state facts about datasets, APIs, free tiers or limits without checking them. Say when something is unverified.
- Do not invent numbers. Results come from runs the owner can reproduce.
- Never put secrets or credentials in the repo.
- Keep docs short and simple.
