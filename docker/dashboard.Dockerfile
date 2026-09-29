FROM python:3.12-slim

WORKDIR /app
COPY requirements-dashboard.txt /tmp/requirements-dashboard.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dashboard.txt
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
