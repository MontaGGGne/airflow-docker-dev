# FROM apache/airflow:2.0.1
# FROM apache/airflow:2.9.1-python3.10
FROM apache/airflow:2.0.1-python3.8
COPY requirements.txt /requirements.txt
COPY for_pip_install/* .
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
# RUN pip install ./preprocess_data-0.0.4-py3-none-any.whl
RUN pip install --no-cache-dir --user -i https://test.pypi.org/simple/ prepData