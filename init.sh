sudo apt-get update
sudo apt-get install ffmpeg libsm6 libxext6 libzbar0 -y
pip install -r requirements.txt

WORK_DIR="/home/onyxia/work"
CLONE_DIR="${WORK_DIR}/funathon-sujet4"

# Clone course repository
REPO_URL="https://github.com/InseeFrLab/funathon2023_sujet4.git"
git clone --depth 1 $REPO_URL $CLONE_DIR

# move important files one level up
cp "${CLONE_DIR}/index.ipynb" "${WORK_DIR}/index.ipynb"
cp "${CLONE_DIR}/app.py" "${WORK_DIR}/app.py"
cp "${CLONE_DIR}/requirements.txt" "${WORK_DIR}/requirements.txt"
cp "${CLONE_DIR}/style.css" "${WORK_DIR}/style.css"
cp "${CLONE_DIR}/argo.yaml" "${WORK_DIR}/argo.yaml"
cp "${CLONE_DIR}/Dockerfile" "${WORK_DIR}/Dockerfile"
cp -R "${CLONE_DIR}/utils" "${WORK_DIR}/utils"
cp -R "${CLONE_DIR}/deployment" "${WORK_DIR}/deployment"


# Open the relevant notebook when starting Jupyter Lab
jupyter server --generate-config
echo "c.LabApp.default_url = '/lab/tree/index.ipynb'" >> /home/onyxia/.jupyter/jupyter_server_config.py