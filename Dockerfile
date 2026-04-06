FROM python:3.12-slim

WORKDIR /app

# Install macboard_adapter SDK first (editable or from wheel)
COPY --from=macboard_adapter_src /macboard_adapter /macboard_adapter
RUN pip install --no-cache-dir /macboard_adapter

# Install adapter dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir -e .

# Copy application code
COPY app/ app/

EXPOSE 8200

# Environment variables:
#   GTFS_URL          — URL of the GTFS static zip (required)
#   GTFS_CACHE_DIR    — local cache directory (default: /tmp/macboard_gtfs)
#   GTFS_STOP_IDS     — optional comma-separated stop_id filter
#   GTFS_CONFIG_YAML  — full YAML config block (overrides individual env vars)

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8200"]
