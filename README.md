# Disney Ad Ops Automation Lab

A complete weekend demo project for Isaac Buziba to simulate the primary automation workflows at Disney Streaming Growth Marketing.

## Architecture Diagram
```
[Airtable UI (BOAT)] <-> (AirtableClient) <-> [Orchestrator]
                                                   |
                                            +------+------+
                                            |             |
                                    [EVE Engine]   [QA Simulator]
                                            |             |
                                            +------+------+
                                                   |
                                            [Alert Pipeline] -> (Slack/Teams)
```

## Getting Started

1. **Setup Environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Generate Data**:
   Run the data generator to seed the `data/` directory with `*.csv` files:
   ```bash
   python src/data_generator.py
   ```
   > 11 files will be created representing real reference structures.

3. **Configure Airtable**:
   Follow the guide in `AIRTABLE_SETUP.md` to map properties and fields into your base, enabling the Airtable features simulated by this lab.

4. **Run Application Demo**:
   Execute the orchestrator demo to simulate the trafficking and QA process.
   ```bash
   python src/orchestrator.py
   ```

5. **Test**:
   To ensure engine capabilities match the required outputs:
   ```bash
   python -m pytest tests/
   ```

## Phases
- **Phase 0:** Core logic setup and automated generation of synthetic production parameters.
- **Phase 1 & 2:** Databricks notebooks providing interactive queries for campaign delta tables.
- **Phase 3:** Internal backend (EVE simulation) including automated payload testing on simulated tasks.
- **Phase 4:** Setup guides on reproducing the Airtable Base exactly reflecting current BOAT tracking.
- **Phase 5:** Formal unittesting across operations.
