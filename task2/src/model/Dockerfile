FROM python:3.8-slim
COPY ./ /home/
WORKDIR /home/
RUN python -m pip install --upgrade pip && pip install -r model_requirements.txt
ENTRYPOINT ["python", "./src/model/model.py"]
