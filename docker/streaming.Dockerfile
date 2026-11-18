FROM python:3.12-slim

WORKDIR /app
COPY requirements-streaming.txt /tmp/requirements-streaming.txt
RUN pip install --no-cache-dir -r /tmp/requirements-streaming.txt
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    MPLBACKEND=Agg
