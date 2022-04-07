# Best practice: Choose a stable base image and tag.
FROM python:3.10-slim-buster

WORKDIR /code

COPY ./requirements/main.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Add repository code
COPY . /code
RUN pip install --no-cache-dir --editable .

CMD ["uvicorn", "nmdc_runtime.api.main:app", "--proxy-headers", "--host", "0.0.0.0", "--port", "8000"]