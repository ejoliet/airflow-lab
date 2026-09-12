FROM python:3.11-slim
RUN useradd -m -u 1000 appuser
WORKDIR /app
RUN pip install --no-cache-dir boto3 requests
COPY --chown=appuser:appuser poller.py .
USER appuser
CMD ["python", "poller.py"]
