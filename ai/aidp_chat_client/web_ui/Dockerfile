# Build context is the parent aidp_chat_client/ directory (set by deploy.py),
# so this Dockerfile sees both the library source and the web_ui/ folder.
FROM python:3.11-slim

# Don't buffer stdout/stderr — without this, Flask request-handler prints can
# sit in a buffer for ages before showing up in container logs, making
# diagnostics painful.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app/

RUN pip install --no-cache-dir . && \
    pip install --no-cache-dir -r web_ui/requirements.txt

WORKDIR /app/web_ui

EXPOSE 5001

CMD ["python", "server.py"]
