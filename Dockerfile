FROM python:3

COPY requirements.txt /requirements.txt
RUN pip3 install -r requirements.txt

COPY /container_folder ./container_folder

WORKDIR /container_folder/newyorker_task/

CMD python -u main/task/execute.py