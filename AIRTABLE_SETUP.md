# Airtable Setup Guide

## Tables to Create (9 total)
Import CSVs in this order:
1. **Markets** ← `08_markets.csv`
2. **Brand Mapping** ← `06_brand_mapping.csv`
3. **Channel Mapping** ← `07_channel_mapping.csv`
4. **Users** ← `09_users.csv`
5. **Ticket Types** ← `10_ticket_types.csv`
6. **Audiences** ← `11_audiences.csv`
7. **Titles** ← `01_titles.csv`
8. **Campaigns** ← `02_campaigns.csv`
9. **Tickets** ← `04_tickets.csv`
10. **QA Checks** ← `05_qa_checks.csv`

## Field Type Conversions
Convert fields from "Single line text" to correct types for each table:
- **Single Select**: Region, Status, Platform, Request Type, Urgency, Stage, Result
- **Date**: Release Date, Start Date, End Date, Created Date
- **Currency**: Budget USD, Spend USD
- **Number**: Impressions, Clicks, Budget
- **Checkbox**: EVE Eligible
- **Link to another record**: Brand Code, Title, Targeting Geo, Channel

## Linked Records to Create
- `Titles.Brand` → Link to **Brand Mapping**
- `Campaigns.Title` → Link to **Titles**
- `Campaigns.Targeting Geo` → Link to **Markets**
- `Campaigns.Channel` → Link to **Channel Mapping**
- `Tickets.Campaign` → Link to **Campaigns**
- `QA Checks.Ticket` → Link to **Tickets**

## Formulas

### SLA Status (Tickets table)
```
IF(IS_AFTER(NOW(), DATEADD({Created Date}, {SLA Hours}, "hours")),
  "Breached",
  IF(IS_AFTER(NOW(), DATEADD({Created Date}, {SLA Hours} * 0.75, "hours")),
    "At Risk",
    "On Track"
  )
)
```

### Needs QA? (Tickets table)
```
IF(
  AND(
    OR({Stage} = "Ready to Launch", {Stage} = "QA"),
    OR(
      FIND("Fail", ARRAYJOIN({QA Results})) > 0,
      FIND("Needs Review", ARRAYJOIN({QA Results})) > 0
    )
  ),
  "BLOCKED - QA Issues",
  ""
)
```

## Views to Create (12 views)

| View | Table | Type | Filter | Group/Sort |
|------|-------|------|--------|------------|
| My Queue | Tickets | Grid | Assignee = Isaac, Stage ≠ Completed | Sort: Due Date asc |
| Kanban Board | Tickets | Kanban | None | Group: Stage |
| SLA Dashboard | Tickets | Grid | SLA Status = Breached OR At Risk | Group: Urgency |
| EVE Ready | Tickets | Grid | EVE Eligible = TRUE, Stage = Trafficking | Sort: Created Date asc |
| Engineer Backlog | Tickets | Grid | Routed To Role = Engineer, Stage ≠ Completed | Sort: Urgency, Due Date |
| By Region | Tickets | Grid | None | Group: Region (lookup) |
| By Platform | Tickets | Grid | None | Group: Platform |
| QA Gate | Tickets | Grid | Stage = QA OR Needs QA? not empty | Sort: Urgency |
| Campaign Calendar | Campaigns | Calendar | None | Date: Start Date |
| Release Slate | Titles | Calendar | None | Date: Release Date |
| Global Campaigns | Campaigns | Grid | None | Group: Region, then Channel |
| Brand Performance | Campaigns | Grid | None | Group: Brand Code |

## Quick Automations to Build
1. **SLA Breach Alert:** Trigger when a record reaches "Breached" SLA Status. Sends an email/Slack message.
2. **QA Gate:** Triggers when Stage changes to Ready to Launch but Needs QA? is populated. Reverts Stage back to "QA" and pings Assignee.
3. **Ticket Auto-Routing:** Trigger when new Ticket is created. Auto-assigns based on `Routed To Role`.
4. **EVE Flagging:** Set EVE Eligible checkbox to TRUE when Request Type matches an eligible automated ticket format.

## Color Coding
For Ticket Views:
- **Red:** SLA Status = "Breached"
- **Orange:** SLA Status = "At Risk"
- **Gray:** Stage = "Blocked"
- **Purple:** EVE Eligible & Stage = "Trafficking"
- **Yellow:** Assignee is empty
