FROM python:3.6

RUN echo "Asia/Shanghai" > /etc/timezone \
 && rm /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

ENV PYTHONPATH=/app

COPY requirements.txt /app/
RUN pip install --upgrade pip \
 && pip install wheel \
 && pip install -r /app/requirements.txt \
 && rm -rf ~/.cache/pip

COPY . /app/

EXPOSE 1129

# Use docker execute command to add gunicore run
CMD ["gunicorn", "-b", "0.0.0.0:1129", "--workers", "2", "--threads", "8", "--worker-connections", "200" ,"run"]
