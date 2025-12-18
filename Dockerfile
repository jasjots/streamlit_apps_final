FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose Streamlit default port
EXPOSE 8080

# Run Streamlit
CMD ["streamlit", "run", "3_Home.py", "--server.address=0.0.0.0", "--server.port=8080"]
