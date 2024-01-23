FROM python

WORKDIR sakura

COPY . /sakura


RUN pip install -r requirements.txt

CMD ["python", "./sakura/mp3-cutter.py"]

FROM alpine
RUN apk update
RUN apk upgrade
RUN apk add --no-cache ffmpeg
