# FROM apache/airflow:2.0.1
FROM apache/airflow:2.9.1-python3.10
# FROM apache/airflow:2.0.1-python3.8
# FROM apache/airflow:2.3.0-python3.10
# FROM apache/airflow:2.9.1-python3.11


COPY requirements.txt /requirements.txt
COPY for_pip_install/* .
# RUN pip install --user --upgrade pip
RUN pip install --upgrade pip

# RUN pip uninstall keras
# RUN pip uninstall tensorflow
# RUN pip cache purge
# RUN pip install --no-cache-dir --user tensorflow

# RUN pip install --no-cache-dir --user -r /requirements.txt

RUN pip uninstall keras
RUN pip uninstall tensorflow
RUN pip cache purge

RUN pip install ./train_and_valid_model-0.2.15-py3-none-any.whl
RUN pip install ./preprocess_data-0.1.10-py3-none-any.whl


RUN pip install -r /requirements.txt

# RUN mlflow server --host 0.0.0.0 --port 5000
# RUN pip install ./preprocess_data-0.0.4-py3-none-any.whl

# RUN pip uninstall tensorflow

# RUN pip install --no-cache-dir --user setuptools==57.5.0

# RUN pip install --no-cache-dir --user tensorflow==2.16.1
# RUN pip install --upgrade optree

# RUN pip uninstall setuptools -y
# RUN pip install --no-cache-dir --user setuptools==57.5.0

# RUN pip uninstall preprocess-data
# RUN pip uninstall train-and-valid-model
# RUN pip install --no-cache-dir --user -i https://test.pypi.org/simple/ preprocess-data
# RUN pip install --no-cache-dir --user -i https://test.pypi.org/simple/ train-and-valid-model
# RUN pip install --no-cache-dir --user ./preprocess_data-0.1.1-py3-none-any.whl
# RUN pip install --no-cache-dir --user ./train_and_valid_model-0.2.8-py3-none-any.whl


