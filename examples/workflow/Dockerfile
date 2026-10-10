FROM python:3.12.15-slim-bookworm@sha256:54c85f3c47607a77f32adec749d3c81d1348bf25833671f512b26a9b6d778cb3
COPY worker.py /worker.py
ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["python3", "/worker.py"]
