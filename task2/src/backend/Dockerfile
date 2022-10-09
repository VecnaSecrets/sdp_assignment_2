FROM python:3.8-slim
COPY ./ /home/
WORKDIR /home/
RUN python -m pip install --upgrade pip && pip install -r backend_requirements.txt
ENTRYPOINT ["python", "./src/backend/python_backend.py"]
# CMD ["python", "./src/model/model.py"]
ENV PYTHONPATH "${PYTHONPATH}:./src/backend/utils/"