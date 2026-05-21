## Minikube Way (the Previous was KIND)

```
# Step 1: Point terminal to Minikube's Docker
eval $(minikube docker-env)

# Step 2: Verify you're now inside Minikube's Docker
# (you should see Minikube's images, NOT your Windows ones)
docker images

# Step 3: Build images INSIDE Minikube
mvn clean package -DskipTests
docker build -f docker/Dockerfile.weather-station -t weather-station:latest .
docker build -f docker/Dockerfile.central-station -t central-station:latest .

# Step 4: Delete old broken pods
kubectl delete -f k8s/

# Step 5: Reapply
kubectl apply -f k8s/persistent-volume.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/central-station.yaml
kubectl apply -f k8s/weather-station.yaml

# Step 6: Watch
kubectl get pods -w
```

```
# ✅ Safe restart (data survives):
kubectl delete pod <central-station-pod>
# K8s automatically creates a new pod, PVC still there, data still there

# ✅ Safe redeploy (data survives):
kubectl delete -f k8s/central-station.yaml
kubectl delete -f k8s/weather-station.yaml
kubectl delete -f k8s/kafka.yaml
# Then reapply — PVC still exists, data still there

# ❌ Full wipe (data gone):
kubectl delete -f k8s/
# This deletes the PVC too — all BitCask and Parquet files gone
```

## Mount

minikube mount ./Archive:/data/weather-data/parquet
minikube mount ./data:/data/weather-data/bitcask
minikube image load central-station:latest


## Bash
kubectl exec -it central-station-7565cd47dd-fk26m  -- /bin/bash

## Logs
kubectl logs central-station-7565cd47dd-fk26m  -f

## JFR yml config
- name: JAVA_TOOL_OPTIONS
              value: "-XX:StartFlightRecording=duration=60s,filename=/data/profile.jfr"

## Copy JFR
kubectl cp <NEW_POD_NAME>:/data/profile.jfr ./profile.jfr -c central-station