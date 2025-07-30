FROM python:3.12.3

# Setting the working directory within the container
WORKDIR /app

# Copying the current working dir into the /app folder
COPY . /app

# Install dependencies
RUN pip install -r requirements.txt


# On starting, what should the container do...
ENTRYPOINT ["python", "app.py"]
#ENTRYPOINT ["python", "app.py", "--mode", "sql"]