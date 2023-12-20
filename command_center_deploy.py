"""
The deploys SQL and Snowpark based store procedures to
the assigned snowflake workspace.
"""





import os
import sys


ignore_folders = ["__pycache__", ".ipynb_checkpoints"]
if len(sys.argv) != 2:
    print("Root directory is required")
    sys.exit()

root_directory = sys.argv[1]
print(f"Deploying all Snowpark apps in root directory {root_directory}")
for directory_path, directory_names, file_names in os.walk(root_directory):
    # Get the last folder name in the directory path
    base_name = os.path.basename(directory_path)
    if base_name in ignore_folders:
        continue

    # If sql based store procedure is present then deploy to workspace
    files_in = [
        f for f in file_names if ".sql" in f and f not in ("setup.sql", "misc.sql")
    ]
    if files_in:
        print(f"Found sql based store procedure in folder {directory_path}")
        for f in files_in:
            file_path = os.path.join(directory_path, f)
            os.chdir(f"{directory_path}")
            os.system(f"snow login -c {root_directory}/config -C main")
            os.system(f"snow sql --filename={file_path}")

    # If python based store procedure is present then deploy to workspace
    if "app.toml" not in file_names:
        continue
    else:
        print(f"Found python based store procedure in folder {directory_path}")
        os.chdir(f"{directory_path}")
        os.system(f"snow login -c {root_directory}/config -C main")
        os.system(f"snow procedure create")

        # Delete uneccessary files from project repo
        try:
            os.remove(os.path.join(directory_path, "app.zip"))
            os.remove(os.path.join(directory_path, "requirements.snowflake.txt"))
        except:
            continue

try:
    os.remove(os.path.join(root_directory, "app.toml"))
except:
    print("app.toml not present in root directory.")
