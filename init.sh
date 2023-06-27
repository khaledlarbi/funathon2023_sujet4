sudo apt-get update
sudo apt-get install ffmpeg libsm6 libxext6 libzbar0 -y
pip install -r requirements.txt

WORK_DIR="/home/onyxia/work"
CLONE_DIR="${WORK_DIR}/funathon-sujet4"

# Clone course repository
REPO_URL="https://github.com/InseeFrLab/funathon2023_sujet4.git"
git clone --depth 1 $REPO_URL $CLONE_DIR

# Open the relevant notebook when starting Jupyter Lab
jupyter server --generate-config
echo "c.LabApp.default_url = '/lab/tree/funathon-sujet4/index.ipynb'" >> /home/onyxia/.jupyter/jupyter_server_config.py
