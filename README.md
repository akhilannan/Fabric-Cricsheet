# Fabric-Cricsheet

Fabric-Cricsheet is a project that aims to build an end-to-end Lakehouse solution using Microsoft Fabric, a cloud-native data platform that enables you to ingest, store, process, and analyze data at scale. Fabric-Cricsheet uses [Cricsheet](https://cricsheet.org/downloads/) data, a collection of ball-by-ball match data for various formats and competitions of cricket, to provide insights and visualizations for cricket fans and analysts.

## Features

- Browse and search for matches by team, format, competition, venue, date, and result
- View detailed scorecards and summary statistics for each match
- Visualize the Player Stats and Match Stats

## Setup

To setup Fabric-Cricsheet, follow these steps:

1. Clone this repository
2. Navigate to the project directory
3. Upload the Notebooks to Microsoft Fabric Workspace
4. Run the Cricsheet Orchestrator notebook from Microsoft Fabric
5. Get the Connection string and SQL analytics endpoint id of clean_lakehouse
6. Open the Semantic Model in Tabular Editor and upate the connection (DatabaseQuery in Shared Expression)
7. Deploy the Cricsheet Model to Fabric Workspace via XMLA endpoint
8. Open the Power BI report and point to the Cricsheet Model in Power BI Service

## License

Fabric-Cricsheet is licensed under the [Creative Commons Attribution 4.0 International License]. You are free to share and adapt the data, as long as you give appropriate credit, provide a link to the license, and indicate if changes were made. You may do so in any reasonable manner, but not in any way that suggests the licensor endorses you or your use.

[Creative Commons Attribution 4.0 International License]: https://creativecommons.org/licenses/by/4.0/