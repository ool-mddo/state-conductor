FROM python:3.12-slim

RUN mkdir -p /state-conductor
WORKDIR /state-conductor
COPY . /state-conductor/

RUN pip install --no-cache-dir poetry
RUN poetry install --no-dev --no-cache
ENV PYTHONPATH=/bgp-policy-parser/src
ENTRYPOINT ["poetry", "run", "python3", "src/app.py"]
