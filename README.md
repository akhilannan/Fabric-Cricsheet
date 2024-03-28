# Fabric-Cricsheet

Fabric-Cricsheet is a project that aims to build an end-to-end Lakehouse solution using Microsoft Fabric, a cloud-native data platform that enables you to ingest, store, process, and analyze data at scale. Fabric-Cricsheet uses [Cricsheet](https://cricsheet.org/downloads/) data, a collection of ball-by-ball match data for various formats and competitions of cricket, to provide insights and visualizations for cricket fans and analysts.

## Features

- Browse and search for matches by team, format, competition, venue, date, and result
- View detailed scorecards and summary statistics for each match
- Visualize the Player Stats and Match Stats

## Setup

To set up Fabric-Cricsheet, follow these steps:

1. Download the "Deploy Cricsheet" notebook from the Deploy folder of the repo.
2. Upload the "Deploy Cricsheet" notebook to your Microsoft Fabric Workspace, ensuring a Fabric Capacity (or Trial) is assigned.
3. Open the notebook. If necessary, modify the Raw/Clean Lakehouse names, then execute the notebook. This process will create several new Fabric items in your workspace.
4. Locate and execute the "Cricsheet Orchestrator" notebook within your workspace.
5. To view the live data load statistics, open the 'lh_bronze' Sql Analytics endpoint, execute the 'job_details.sql' script located in the 'Sql' folder of the repo.
6. Once Data Load is complete, open the "Cricsheet Analysis" Power BI report from your Workspace to verify data retrieval.