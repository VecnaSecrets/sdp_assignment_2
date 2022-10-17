FROM amd64/python:3.8.6-slim
COPY ./ /home/
WORKDIR /home/
RUN python3 -m pip install --upgrade pip && pip install -r backend_requirements.txt
ENTRYPOINT ["python", "./src/backend/python_backend.py"]
# CMD ["python", "./src/model/model.py"]
ENV PYTHONPATH "${PYTHONPATH}:./src/backend/utils/"
ENV PYTHONPATH "${PYTHONPATH}:./src/backend/data/"