FROM amd64/python:3.8.6-slim
COPY ./ /home/
WORKDIR /home/
RUN pip install -U pip
RUN pip install --upgrade setuptools
RUN pip3 install catboost
RUN python3 -m pip install --upgrade pip && pip install -r model_requirements.txt

ENTRYPOINT ["python", "./src/model/model.py"]
