FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir grpcio grpcio-tools
COPY . .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bioskop.proto
ENV PORT=50051 DB=/data/bioskop.db
CMD ["python", "server.py"]
