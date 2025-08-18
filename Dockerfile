FROM python:3.11

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV FLASK_APP=app.py
ENV PORT=8080

RUN pip install --upgrade pip

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Create data directory if it doesn't exist
RUN mkdir -p data

# Make sure Skeleton Output.xlsx exists
# (No need to do anything if it's already in your repository)

# Expose port
EXPOSE 8080

# Run the application
CMD gunicorn --bind 0.0.0.0:8080 app:app