FROM python:3-onbuild

ARG python_version=3.5.2


RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install  -U git+https://2b10da91b56a6aac4dc07632dc2597992e8839cc:x-oauth-basic@github.com/suguroglu/hds-data-science-utils.git@dev
ENV PYTHONPATH='$PYTHONPATH'
ADD /data /data

CMD python ts_analysis.py