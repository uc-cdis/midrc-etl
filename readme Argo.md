Code Snippits

#Docker
docker build -t midrc-jupyter-minimal-notebook:latest .
docker run -it --rm midrc-jupyter-minimal-notebook:latest
Pressing Ctrl-C twice shuts down the notebook server but leaves the container intact on disk for later restart or permanent deletion using commands like the following:
docker ps -q --filter ancestor="midrc-jupyter-minimal-notebook:latest" | xargs -r docker stop

#conda Create New Envoirnment
conda create --name midrc
conda init bash
conda activate midrc
RUN conda env create -f environment.yml

#Install Libraries
conda install pandas
python3 -m pip install papermill

#Docker File
RUN conda create --name midrc
RUN conda activate midrc
RUN conda install pandas
RUN python3 -m pip install papermill

papermill checkColumns.ipynb out.ipynb -p folderPath './data/RSNA/'
