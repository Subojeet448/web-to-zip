# Yeh Playwright ka official image hai jisme sab kuch pehle se hai
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

WORKDIR /app

# Requirements copy aur install karo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Apna Python code copy karo
COPY . .

# Server start karne ka command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "10000"]
