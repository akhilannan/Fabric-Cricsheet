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
3. Open the notebook. If necessary, modify details such as Lakehouse names, schema names, workspace name, etc., and then execute the notebook. 
4. The execution will create several new Fabric items in the Cricsheet workspace. It will also start the data load process.
5. To view the live data load statistics, open the "Data Load Monitor" report in Cricsheet workspace. Alternatively, you can access the 'lh_cricsheet' SQL Analytics endpoint and execute the 'job_details.sql' script located in the 'Sql' folder of the repository.
6. Once Data Load is complete, open the "Cricsheet Analysis" Power BI report from Cricsheet Workspace to verify data retrieval.


## Alternative Setup: Run Locally with Environment Variables

If you'd prefer to clone the repository and run the notebook locally, follow these steps:

1. **Clone the repository** to your local machine:
   ```bash
   git clone <repository-url>
   ```
2. **Set up environment variables** for your credentials in your development environment (such as VS Code). Ensure that these variables include the necessary credentials for accessing your Fabric resources.
3. **Open the notebook** in VS Code (or another Jupyter-compatible environment), and execute it locally with the defined environment variables. Modify any necessary details such as Lakehouse names, schema names, workspace names, etc., as required.

> **Note:** If you follow the standard setup (steps 1 and 2 in the **Setup** section), these credentials are not required, as the notebook will use Semantic Link for authentication.