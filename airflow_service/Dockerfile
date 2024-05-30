FROM apache/airflow:2.0.1
COPY requirements.txt /requirements.txt
COPY for_pip_install/preprocess_data-0.0.4-py3-none-any.whl /for_pip_install/preprocess_data-0.0.4-py3-none-any.whl
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt
RUN pip install /for_pip_install/preprocess_data-0.0.4-py3-none-any.whl