# Microsoft ka official image (Playwright 1.42.0 aur saare browsers pehle se hain)
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# App directory set karo
WORKDIR /app

# Pehle requirements copy karo aur install karo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Phir apna main app.py code copy karo
COPY . .

# App start karne ka command (tumhara code khud $PORT handle kar lega)
CMD ["python", "app.py"]
