# Kubernetes Control Guide

This guide explains how to turn off and turn on the local Kubernetes environment used by this project.

## Cluster name

The example cluster name used in this repo is:

- `weather-dev`

If you created a different name, replace `weather-dev` with your cluster name.

---

## Turn off Kubernetes

### Recommended: delete the kind cluster

This is the cleanest way to stop Kubernetes and remove the local cluster state.

```powershell
kind delete cluster --name weather-dev
```

### Alternative: stop the kind node container

If you want to pause the cluster without deleting it, stop the Docker container(s) for the kind node(s):

```powershell
docker ps --filter "name=weather-dev" --format "{{.Names}}"
docker stop <node-name>
```

Common node names include:

- `weather-dev-control-plane`
- `weather-dev-worker` (if present)

> Note: stopping the container may make the cluster unavailable, but it preserves the cluster metadata. If you want a clean restart, delete and recreate instead.

---

## Turn on Kubernetes

### If you deleted the cluster

Recreate the local kind cluster:

```powershell
kind create cluster --name weather-dev
```

Then reload the images and reapply the manifests:

```powershell
kind load docker-image weather-station:latest --name weather-dev
kind load docker-image central-station:latest --name weather-dev
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/central-station.yaml
kubectl apply -f k8s/weather-station.yaml
```

### If you stopped the node container(s)

Start the Docker container(s) again:

```powershell
docker start <node-name>
```

Then verify the cluster is available:

```powershell
kubectl get nodes
kubectl get pods -A
```

---

## Verify the cluster

Check node status:

```powershell
kubectl get nodes
```

Check application pods:

```powershell
kubectl get pods
```

---

## Notes

- If your cluster was deleted, you must reload your built Docker images into kind before deploying.
- If you are using a different cluster tool than kind, adjust the commands accordingly.
- For live log monitoring after startup, use:

```powershell
kubectl logs -l app=weather-station --tail=100 -f
kubectl logs deployment/central-station --tail=100 -f
```
