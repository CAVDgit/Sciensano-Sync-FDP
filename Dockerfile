FROM python:3.12-slim

# System deps (if needed later).
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     curl \
#  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 1. Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy code
COPY . .

# Make sure Python output is unbuffered (logs visible in `docker logs`)
ENV PYTHONUNBUFFERED=1

# 3. Default command = supervisor loop
CMD ["python", "supervisor.py"]
