FROM python:3.12-slim

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 8000/tcp: API + Swagger UI; 4789+4790/udp: the two local VXLAN receivers
EXPOSE 8000/tcp 4789/udp 4790/udp

# Per-egress pending-frame limits are set at startup here (override in compose).
ENV EGRESS_A_CAPACITY=100 \
    EGRESS_B_CAPACITY=100 \
    SEND_INTERVAL=0.001 \
    WRITE_RETRY_INTERVAL=0.06

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
