import os
import wandb
import psycopg2
import pandas as pd
import json
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')
logger = logging.getLogger(__name__)

# Number of threads (adjust based on system and database limits)
MAX_WORKERS = 20


def get_db_connection():
    """Establish a connection to the PostgreSQL database using environment variables."""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST_OHO"),
            port=os.getenv("PGPORT"),
            database=os.getenv("POSTGRES_DB_OHO"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise


def create_tables(conn):
    """Create necessary tables in PostgreSQL if they don't exist."""
    create_tables_sql = """
    CREATE TABLE IF NOT EXISTS runs (
        run_id VARCHAR(50) PRIMARY KEY,
        group_id VARCHAR(100),
        run_name VARCHAR(255),
        run_url TEXT,
        created_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS run_config (
        run_id VARCHAR(50),
        config_key VARCHAR(255),
        config_value JSONB,
        PRIMARY KEY (run_id, config_key),
        FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );

    CREATE TABLE IF NOT EXISTS summary_metrics (
        run_id VARCHAR(50),
        metric_name VARCHAR(255),
        metric_value JSONB,
        PRIMARY KEY (run_id, metric_name),
        FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );

    CREATE TABLE IF NOT EXISTS run_history (
        run_id VARCHAR(50),
        step INTEGER,
        timestamp TIMESTAMP,
        PRIMARY KEY (run_id, step),
        FOREIGN KEY (run_id) REFERENCES runs(run_id)
    );

    CREATE TABLE IF NOT EXISTS history_metrics (
        run_id VARCHAR(50),
        step INTEGER,
        metric_name VARCHAR(255),
        metric_value JSONB,
        PRIMARY KEY (run_id, step, metric_name),
        FOREIGN KEY (run_id, step) REFERENCES run_history(run_id, step)
    );

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_runs_group_id ON runs(group_id);
    CREATE INDEX IF NOT EXISTS idx_run_config_run_id ON run_config(run_id);
    CREATE INDEX IF NOT EXISTS idx_run_config_key ON run_config(config_key);
    CREATE INDEX IF NOT EXISTS idx_summary_metrics_run_id ON summary_metrics(run_id);
    CREATE INDEX IF NOT EXISTS idx_run_history_run_id ON run_history(run_id);
    CREATE INDEX IF NOT EXISTS idx_history_metrics_run_id_step ON history_metrics(run_id, step);
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_tables_sql)
            conn.commit()
        logger.info("Database tables created or verified")
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        conn.rollback()
        raise


def fetch_wandb_runs(group_id, entity, project):
    """Fetch runs from W&B for a specific group ID or entire project."""
    api = wandb.Api()
    try:
        filters = {"group": group_id} if group_id else {}
        runs = api.runs(
            path=f"{entity}/{project}",
            filters=filters
        )
        run_list = list(runs)  # Convert iterator to list for parallel processing
        logger.info(f"Fetched {len(run_list)} runs for project {entity}/{project}" + (
            f" with group ID {group_id}" if group_id else ""))
        return run_list
    except Exception as e:
        logger.error(f"Failed to fetch runs from W&B: {e}")
        raise


def store_run_data(run, conn_params):
    """Store run metadata, config, summary, and history in PostgreSQL."""
    thread_name = threading.current_thread().name
    try:
        # Create a new connection for this thread
        conn = psycopg2.connect(**conn_params)
        with conn.cursor() as cur:
            # Insert run metadata
            cur.execute("""
                INSERT INTO runs (run_id, group_id, run_name, run_url, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO NOTHING
            """, (
                run.id,
                run.group,
                run.name,
                run.url,
                run.created_at
            ))

            # Insert config key-value pairs
            for key, value in run.config.items():
                cur.execute("""
                    INSERT INTO run_config (run_id, config_key, config_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, config_key) DO NOTHING
                """, (
                    run.id,
                    key,
                    json.dumps(value)
                ))

            # Insert summary metrics
            for metric_name, metric_value in run.summary.items():
                cur.execute("""
                    INSERT INTO summary_metrics (run_id, metric_name, metric_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, metric_name) DO NOTHING
                """, (
                    run.id,
                    metric_name,
                    json.dumps(metric_value)
                ))

            # Fetch history from parquet file
            try:
                # Initialize W&B run context to use artifacts
                wandb_run = wandb.init(project=run.project, entity=run.entity, id=run.id, resume="must")
                artifact_name = f"{run.entity}/{run.project}/run-{run.id}-history:v0"
                artifact = wandb_run.use_artifact(artifact_name, type='wandb-history')

                # Download artifact to /dump
                artifact_dir = artifact.download(root="/dump")

                # Find the parquet file in the /dump directory
                parquet_files = [f for f in os.listdir(artifact_dir) if f.endswith('.parquet')]
                if not parquet_files:
                    raise FileNotFoundError("No parquet file found in artifact directory")

                parquet_path = os.path.join(artifact_dir, parquet_files[0])
                history_df = pd.read_parquet(parquet_path)
                logger.info(f"[{thread_name}] Loaded history from parquet for run {run.id} in /dump")

                # Finish the W&B run context
                wandb_run.finish()
            except Exception as e:
                logger.warning(
                    f"[{thread_name}] Failed to load parquet for run {run.id}: {e}. Falling back to scan_history.")
                # Fallback to scan_history for running runs or if artifact is unavailable
                history_data = run.scan_history()
                history_df = pd.DataFrame(history_data)
                if history_df.empty:
                    logger.warning(f"[{thread_name}] No history data available for run {run.id}")
                    conn.commit()
                    return

            # Process history DataFrame
            for index, row in history_df.iterrows():
                metrics = {k: v for k, v in row.items() if pd.notna(v)}
                timestamp = row.get('_timestamp', None)
                if timestamp:
                    timestamp = datetime.fromtimestamp(timestamp)

                # Insert history step
                cur.execute("""
                    INSERT INTO run_history (run_id, step, timestamp)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (run_id, step) DO NOTHING
                """, (
                    run.id,
                    index,
                    timestamp
                ))

                # Insert history metrics
                for metric_name, metric_value in metrics.items():
                    cur.execute("""
                        INSERT INTO history_metrics (run_id, step, metric_name, metric_value)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (run_id, step, metric_name) DO NOTHING
                    """, (
                        run.id,
                        index,
                        metric_name,
                        json.dumps(metric_value)
                    ))

            conn.commit()
        logger.info(f"[{thread_name}] Stored data for run {run.id}")
    except Exception as e:
        logger.error(f"[{thread_name}] Failed to store data for run {run.id}: {e}")
        if 'conn' in locals():
            conn.rollback()
        raise
    finally:
        if 'conn' in locals():
            conn.close()


def process_runs(runs, conn_params):
    """Process runs in parallel using ThreadPoolExecutor."""
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="Worker") as executor:
        futures = [executor.submit(store_run_data, run, conn_params) for run in runs]
        for future in as_completed(futures):
            try:
                future.result()  # Raise any exceptions from the thread
            except Exception as e:
                logger.error(f"Thread failed: {e}")


def main(group_id, entity, project):
    """Main function to fetch W&B data and store in PostgreSQL."""
    # Initialize W&B API
    if not os.getenv("WANDB_API_KEY"):
        raise ValueError("WANDB_API_KEY environment variable not set")

    # Get database connection for table creation
    conn = get_db_connection()
    try:
        # Create tables
        create_tables(conn)

        # Prepare connection parameters for threads
        conn_params = {
            "host": os.getenv("DB_HOST_OHO"),
            "port": os.getenv("PGPORT"),
            "database": os.getenv("POSTGRES_DB_OHO"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD")
        }
        # Fetch runs
        runs = fetch_wandb_runs(group_id, entity, project)

        # Process runs in parallel
        logger.info(f"Starting parallel processing with {MAX_WORKERS} workers")
        process_runs(runs, conn_params)

        logger.info("Data transfer completed successfully")
    finally:
        conn.close()
        logger.info("Main database connection closed")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 1 or len(sys.argv) > 3:
        print("Usage: python wandb_to_postgres.py [project] [group_id]")
        sys.exit(1)

    group_id = sys.argv[2] if len(sys.argv) == 3 else None
    project = sys.argv[1]

    try:
        main(group_id, "wlp9800-new-york-university", project)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)
