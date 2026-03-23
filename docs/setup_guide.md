# Setup Guide for Microsoft Fabric Data Pipeline

This guide provides step-by-step instructions to set up your Microsoft Fabric environment to deploy and run the data pipeline. It covers prerequisites, workspace configuration, and data source setup.

## Prerequisites

Before you begin, ensure you have the following:

1.  **Microsoft Fabric Capacity:** Access to a Microsoft Fabric workspace with appropriate capacity (e.g., F64 or higher for production workloads).
2.  **Azure Data Lake Storage Gen2 (ADLS Gen2) Account:** An ADLS Gen2 account to store your raw, bronze, silver, and gold layer data. This will be mounted as a Lakehouse in Fabric.
3.  **Service Principal (Optional but Recommended):** For automated deployments and secure access to ADLS Gen2, consider creating a Service Principal with appropriate permissions (Storage Blob Data Contributor) on your ADLS Gen2 account.
4.  **Permissions:** Ensure you have contributor or administrator roles within your Microsoft Fabric workspace.

## 1. Create a Microsoft Fabric Workspace and Lakehouse

1.  **Navigate to Microsoft Fabric:** Go to the [Microsoft Fabric portal](https://app.fabric.microsoft.com/).
2.  **Create a Workspace:**
    *   Click on `Workspaces` in the left navigation pane.
    *   Click `New workspace`.
    *   Provide a meaningful name (e.g., `DataPipelineWorkspace`) and a description.
    *   Assign the workspace to an appropriate Fabric capacity.
    *   Click `Apply`.
3.  **Create a Lakehouse:**
    *   Within your newly created workspace, click `New` -> `Lakehouse`.
    *   Give your Lakehouse a name (e.g., `MedallionLakehouse`). This Lakehouse will serve as the central storage for your Bronze, Silver, and Gold layers.
    *   Click `Create`.

## 2. Configure Data Sources (Simulated Raw Data)

For this pipeline, we simulate raw data ingestion from files. You will need to upload sample data to your ADLS Gen2 account and ensure it's accessible via the Lakehouse.

1.  **Upload Sample Data to ADLS Gen2:**
    *   Access your ADLS Gen2 account in the Azure portal.
    *   Create a container (e.g., `rawdata`).
    *   Upload your sample raw data files (e.g., `sample_data.csv`) into this container. The `ingest_data.py` notebook expects a CSV-like structure.
2.  **Mount ADLS Gen2 in Fabric (if not automatically linked by Lakehouse):**
    *   When you create a Lakehouse, it automatically creates a `Files` section that points to the underlying ADLS Gen2 storage. Ensure your `rawdata` container is accessible here.
    *   The notebooks use `abfss://<container_name>@<your_storage_account>.dfs.core.windows.net/` paths. Replace `<your_storage_account>` with your actual ADLS Gen2 account name.

## 3. Upload Notebooks and Configuration

1.  **Clone the Repository:** Clone this GitHub repository to your local machine.
    ```bash
    git clone https://github.com/your-repo/microsoft-fabric-data-pipeline.git
    cd microsoft-fabric-data-pipeline
    ```
2.  **Upload Notebooks to Fabric:**
    *   In your Fabric workspace, navigate to the `Data Engineering` persona.
    *   Click `Upload file` or `Import notebook`.
    *   Upload all `.py` files from the `notebooks/bronze`, `notebooks/silver`, `notebooks/gold`, and `notebooks/utils` directories into your Fabric workspace as notebooks. Fabric will recognize them as PySpark notebooks.
    *   **Important:** Ensure the `pipeline_utils.py` notebook is also uploaded. The other notebooks will reference functions from it.
3.  **Upload Configuration File:**
    *   Upload `config/pipeline_config.json` to a location accessible by your notebooks, for example, directly into the `Files` section of your Lakehouse or a dedicated `config` folder within the Lakehouse `Files` section.

## 4. Update Configuration in Notebooks

Open each uploaded notebook in Fabric and update the placeholder paths and configurations.

1.  **Update Storage Account Name:** In `notebooks/bronze/ingest_data.py`, `notebooks/bronze/bronze_layer_processing.py`, `notebooks/silver/silver_layer_processing.py`, `notebooks/gold/gold_layer_processing.py`, `notebooks/gold/llm_view_creation.py`, and `notebooks/gold/reporting_view_creation.py`, replace `<your_storage_account>` with the actual name of your ADLS Gen2 storage account.
    *   Example: `abfss://bronze@**youradlsgen2account**.dfs.core.windows.net/`
2.  **Review `pipeline_config.json`:** Ensure the `pipeline_config.json` file reflects your environment's settings, especially the email recipient for notifications.

## 5. Install Delta Lake (if not pre-installed)

Microsoft Fabric environments typically come with Delta Lake pre-installed. However, if you encounter issues, you might need to ensure the Delta Lake library is available.

*   In a Fabric notebook, you can check installed libraries or install new ones using `%pip` or `%conda` commands if needed. For Delta Lake, it's usually part of the default runtime.

## Next Steps

Once the setup is complete, proceed to the [Usage Guide](usage_guide.md) to learn how to run the pipeline.
