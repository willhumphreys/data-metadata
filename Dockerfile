FROM python:3.13-slim-bullseye

RUN apt-get update && \
    apt-get install -y lzop build-essential python3-dev liblzo2-dev && \
    rm -rf /var/lib/apt/lists/*


# Set the working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . /app

ENV PYTHONPATH="${PYTHONPATH}:/app"

# Set the command to run your application.
# Since runner.py is inside the src directory, adjust the path accordingly.
ENTRYPOINT ["python", "src/orchestrator.py"]