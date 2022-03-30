FROM jupyter/minimal-notebook:latest

USER jovyan

# Setup envoirnment
RUN conda install pandas
RUN python3 -m pip install papermill

# Copy relevant notebooks
COPY --chown=jovyan:users testHelloWorld.ipynb /home/jovyan/testHelloWorld.ipynb
COPY --chown=jovyan:users usecase1 /home/jovyan/usecase1