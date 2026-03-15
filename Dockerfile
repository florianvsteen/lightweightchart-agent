FROM python:3.12-slim

# System deps for playwright + build tools
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    gnupg \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install PM2 globally
RUN npm install -g pm2

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install playwright chromium for Discord screenshots
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy application code
COPY . .

# Create data directory for disk cache
RUN mkdir -p /app/data

# Expose all pair ports + mission control
EXPOSE 6767

# Start via PM2 using install.sh logic
CMD ["pm2-runtime", "start", "app.py", "--name", "lightweightchart-agent", "--interpreter", "python3"]
