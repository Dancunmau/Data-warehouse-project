import subprocess
import os
from dotenv import load_dotenv

def main():
    # Load .env into the active session so dbt can securely read the password!
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(base_dir, ".env"))
    
    print("==================================================")
    print("  Executing Modern Data Stack (dbt + Postgres)    ")
    print("==================================================")
    
    # 1. Run Python EL (Extract & Load Raw CSVs to Postgres)
    print("\n[Stage 1: Python Extract & Load]...")
    subprocess.run(["python", "src/elt/extract_load_db.py"], check=True)
    
    # 2. Run dbt T (Transform dynamically in Postgres)
    print("\n[Stage 2: dbt Transformation Engine]...")
    dbt_project_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dbt_project")
    
    try:
        # Run dbt natively from python
        subprocess.run(["dbt", "run"], cwd=dbt_project_dir, check=True)
        print("\nPipeline execution complete! Data Warehouse is live.")
    except Exception as e:
        print(f"dbt execution failed. Make sure you updated .env DB_PASSWORD! Error: {e}")

if __name__ == "__main__":
    main()
