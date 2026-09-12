# cluster-agent — alert-triggered autonomous reconciler (docs: README.md)
FROM python:3.13-slim

# kubectl — pinned, verified. Matches cluster minor or +/-1 per skew policy.
ARG KUBECTL_VERSION=v1.33.4
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSLo /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl" \
 && chmod +x /usr/local/bin/kubectl \
 && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/

EXPOSE 8080
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
