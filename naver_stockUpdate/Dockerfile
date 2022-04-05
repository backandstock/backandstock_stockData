FROM python:3.8

RUN crontab crontab

ADD ./etc/cronjob.txt   /etc/cron.d/

WORKDIR /usr/src

COPY ./requirements.txt .

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . .

# CMD ["stockUpdate.py"]

# run crond as main process of container
CMD ["cron", "-f"]

ENTRYPOINT ["python3"]