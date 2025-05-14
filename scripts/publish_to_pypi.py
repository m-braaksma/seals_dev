

# DONT USE, use github actions instead. You might be here simply because you forgot to update the version number in setup.py






import shutil
import subprocess
import sys
from pathlib import Path



def run(cmd):
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        sys.exit(result.returncode)

def main():
    project_root = Path(__file__).parent

    # Step 1: Clean previous builds
    for folder in ["build", "dist"]:
        shutil.rmtree(project_root / folder, ignore_errors=True)
    for egg_info in project_root.glob("*.egg-info"):
        shutil.rmtree(egg_info, ignore_errors=True)

    # Step 2: Build distributions
    run(f"{sys.executable} -m pip install --upgrade build")  # optional
    run(f"{sys.executable} -m build")

    # Step 3: Upload to PyPI
    run("twine upload dist/*")

if __name__ == "__main__":
    main()