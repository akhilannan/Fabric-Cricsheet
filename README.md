# Fabric-Cricsheet

Fabric-Cricsheet is a project that aims to build an end-to-end Lakehouse solution using Microsoft Fabric, a cloud-native data platform that enables you to ingest, store, process, and analyze data at scale. Fabric-Cricsheet uses [Cricsheet](https://cricsheet.org/downloads/) data, a collection of ball-by-ball match data for various formats and competitions of cricket, to provide insights and visualizations for cricket fans and analysts.

## Features

- Browse and search for matches by team, format, competition, venue, date, and result
- View detailed scorecards and summary statistics for each match
- Visualize the Player Stats and Match Stats

## Setup

To set up Fabric-Cricsheet, follow these steps:

1. Download the "Deploy Cricsheet" notebook from the Deploy folder.
2. Upload the "Deploy Cricsheet" notebook to your Microsoft Fabric Workspace, ensuring a Fabric Capacity is assigned.
3. Open the notebook. If necessary, modify the Raw/Clean Lakehouse names, then execute the notebook. This process will create several new Fabric items in your workspace.
4. Locate and execute the "Cricsheet Orchestrator" notebook within your workspace.
5. Locate the "Cricsheet Analysis" Power BI report from your Workspace to verify data retrieval.


## License

Fabric-Cricsheet is licensed under the [Creative Commons Attribution 4.0 International License]. You are free to share and adapt the data, as long as you give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.

[Creative Commons Attribution 4.0 International License]: https://creativecommons.org/licenses/by/4.0/