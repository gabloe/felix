# Kubernetes Deployment

This guide covers deploying Felix on Kubernetes with high availability, persistence, and production-grade configurations.

## Overview

Felix is designed as a Kubernetes-native system. This guide demonstrates:

- **StatefulSet deployment** for stable network identity
- **Headless Services** for direct broker addressing
- **Persistent storage** with StatefulSet volumes
- **Resource management** and limits
- **Health checks** and readiness probes
- **Horizontal scaling** for high availability

!!! info "Kubernetes Version"
    Felix requires Kubernetes 1.24 or later. Tested on 1.27+.

## Prerequisites

- **Kubernetes cluster**: 1.24+ (Minikube, kind, GKE, EKS, AKS, etc.)
- **kubectl**: Configured to access your cluster
- **Storage provisioner**: For persistent volumes (e.g., `gp3` on AWS, `pd-ssd` on GCP)
- **4GB RAM per broker pod**: Minimum recommended

### Verify Cluster Access

```bash
kubectl cluster-info
kubectl get nodes
```

## Quick Start

### Basic Deployment

Deploy a single Felix broker:

```bash
# Create namespace
kubectl create namespace felix

# Apply manifests
kubectl apply -f deploy/kubernetes/broker.yaml -n felix

# Check status
kubectl get pods -n felix
kubectl logs -f deployment/felix-broker -n felix
```

**Minimal broker deployment** (`deploy/kubernetes/broker.yaml`):

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: felix

---
apiVersion: v1
kind: Service
metadata:
  name: felix-broker
  namespace: felix
  labels:
    app: felix-broker
spec:
  type: ClusterIP
  ports:
    - port: 5000
      targetPort: 5000
      protocol: UDP
      name: quic
    - port: 8080
      targetPort: 8080
      protocol: TCP
      name: metrics
  selector:
    app: felix-broker

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: felix-broker
  namespace: felix
spec:
  replicas: 1
  selector:
    matchLabels:
      app: felix-broker
  template:
    metadata:
      labels:
        app: felix-broker
    spec:
      containers:
      - name: broker
        image: felix/broker:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 5000
          protocol: UDP
          name: quic
        - containerPort: 8080
          protocol: TCP
          name: metrics
        env:
        - name: FELIX_QUIC_BIND
          value: "0.0.0.0:5000"
        - name: FELIX_BROKER_METRICS_BIND
          value: "0.0.0.0:8080"
        - name: RUST_LOG
          value: "info"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
```

## StatefulSet Deployment

For production, use StatefulSets for stable network identity and persistent storage:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: felix-broker-headless
  namespace: felix
  labels:
    app: felix-broker
spec:
  clusterIP: None
  ports:
    - port: 5000
      protocol: UDP
      name: quic
    - port: 8080
      protocol: TCP
      name: metrics
  selector:
    app: felix-broker

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: felix-broker
  namespace: felix
spec:
  serviceName: felix-broker-headless
  replicas: 3
  selector:
    matchLabels:
      app: felix-broker
  template:
    metadata:
      labels:
        app: felix-broker
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: app
                  operator: In
                  values:
                  - felix-broker
              topologyKey: kubernetes.io/hostname
      containers:
      - name: broker
        image: felix/broker:latest
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 5000
          protocol: UDP
          name: quic
        - containerPort: 8080
          protocol: TCP
          name: metrics
        env:
        - name: FELIX_QUIC_BIND
          value: "0.0.0.0:5000"
        - name: FELIX_BROKER_METRICS_BIND
          value: "0.0.0.0:8080"
        - name: FELIX_EVENT_BATCH_MAX_EVENTS
          value: "64"
        - name: FELIX_EVENT_BATCH_MAX_DELAY_US
          value: "250"
        - name: FELIX_CACHE_CONN_POOL
          value: "8"
        - name: FELIX_CACHE_STREAMS_PER_CONN
          value: "4"
        - name: FELIX_DISABLE_TIMINGS
          value: "false"
        - name: RUST_LOG
          value: "info"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        volumeMounts:
        - name: data
          mountPath: /data
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 15
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 3
        lifecycle:
          preStop:
            exec:
              command: ["/bin/sh", "-c", "sleep 15"]
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 50Gi
```

**Deploy:**

```bash
kubectl apply -f statefulset.yaml
kubectl get statefulset -n felix
kubectl get pods -n felix -l app=felix-broker
```

## Configuration Management

### ConfigMap for Settings

Externalize configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: felix-broker-config
  namespace: felix
data:
  broker.yml: |
    quic_bind: "0.0.0.0:5000"
    metrics_bind: "0.0.0.0:8080"
    event_batch_max_events: 64
    event_batch_max_delay_us: 250
    event_batch_max_bytes: 65536
    fanout_batch_size: 64
    cache_conn_recv_window: 268435456
    cache_stream_recv_window: 67108864
    cache_send_window: 268435456
    pub_workers_per_conn: 4
    pub_queue_depth: 1024
    subscriber_queue_capacity: 128
    subscriber_writer_lanes: 4
    subscriber_lane_queue_depth: 8192
    max_subscriber_writer_lanes: 8
    subscriber_lane_shard: auto
    disable_timings: false

---
# Mount in StatefulSet
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: felix-broker
  namespace: felix
spec:
  template:
    spec:
      containers:
      - name: broker
        env:
        - name: FELIX_BROKER_CONFIG
          value: /etc/felix/broker.yml
        volumeMounts:
        - name: config
          mountPath: /etc/felix
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: felix-broker-config
```

### Secrets for Sensitive Data

Store credentials securely:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: felix-broker-secrets
  namespace: felix
type: Opaque
stringData:
  tls-cert.pem: |
    -----BEGIN CERTIFICATE-----
    ...
    -----END CERTIFICATE-----
  tls-key.pem: |
    -----BEGIN PRIVATE KEY-----
    ...
    -----END PRIVATE KEY-----

---
# Mount in StatefulSet
volumeMounts:
- name: tls-certs
  mountPath: /etc/felix/tls
  readOnly: true

volumes:
- name: tls-certs
  secret:
    secretName: felix-broker-secrets
```

## Storage Configuration

### StorageClass for High Performance

Create optimized storage class:

```yaml
# AWS EBS gp3 with provisioned IOPS
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: felix-fast-ssd
provisioner: ebs.csi.aws.com
parameters:
  type: gp3
  iops: "3000"
  throughput: "125"
  fsType: ext4
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true

---
# GCP persistent disk SSD
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: felix-fast-ssd
provisioner: pd.csi.storage.gke.io
parameters:
  type: pd-ssd
  replication-type: regional-pd
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
```

### PersistentVolumeClaim Templates

Define storage requirements in StatefulSet:

```yaml
volumeClaimTemplates:
- metadata:
    name: data
    labels:
      app: felix-broker
  spec:
    accessModes: ["ReadWriteOnce"]
    storageClassName: felix-fast-ssd
    resources:
      requests:
        storage: 100Gi
```

## Networking

### Service for External Access

Expose broker externally:

```yaml
# LoadBalancer (cloud providers)
apiVersion: v1
kind: Service
metadata:
  name: felix-broker-external
  namespace: felix
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: "nlb"
spec:
  type: LoadBalancer
  ports:
    - port: 5000
      targetPort: 5000
      protocol: UDP
      name: quic
  selector:
    app: felix-broker

---
# NodePort (bare metal)
apiVersion: v1
kind: Service
metadata:
  name: felix-broker-nodeport
  namespace: felix
spec:
  type: NodePort
  ports:
    - port: 5000
      targetPort: 5000
      nodePort: 30500
      protocol: UDP
      name: quic
  selector:
    app: felix-broker
```

### Ingress for HTTP Metrics

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: felix-metrics
  namespace: felix
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
  - host: felix-metrics.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: felix-broker
            port:
              number: 8080
```

## High Availability Setup

### Multi-Zone Deployment

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: felix-broker
  namespace: felix
spec:
  replicas: 3
  template:
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
                - felix-broker
            topologyKey: topology.kubernetes.io/zone
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: topology.kubernetes.io/zone
        whenUnsatisfiable: DoNotSchedule
        labelSelector:
          matchLabels:
            app: felix-broker
```

### Pod Disruption Budget

Protect against voluntary disruptions:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: felix-broker-pdb
  namespace: felix
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: felix-broker
```

### HorizontalPodAutoscaler

Scale based on metrics:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: felix-broker-hpa
  namespace: felix
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: felix-broker
  minReplicas: 3
  maxReplicas: 10
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

## Monitoring and Observability

### ServiceMonitor for Prometheus

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: felix-broker
  namespace: felix
  labels:
    app: felix-broker
spec:
  selector:
    matchLabels:
      app: felix-broker
  endpoints:
  - port: metrics
    interval: 15s
    path: /metrics
```

### Grafana Dashboard ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: felix-dashboard
  namespace: monitoring
  labels:
    grafana_dashboard: "1"
data:
  felix-overview.json: |
    {
      "dashboard": {
        "title": "Felix Broker Overview",
        "panels": [...]
      }
    }
```

## Resource Management

### Quality of Service Classes

**Guaranteed QoS** (production):

```yaml
resources:
  requests:
    memory: "4Gi"
    cpu: "2000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

**Burstable QoS** (development):

```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

### Resource Quotas

Limit namespace resources:

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: felix-quota
  namespace: felix
spec:
  hard:
    requests.cpu: "20"
    requests.memory: "40Gi"
    limits.cpu: "40"
    limits.memory: "80Gi"
    persistentvolumeclaims: "10"
```

### LimitRange

Set default resource limits:

```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: felix-limits
  namespace: felix
spec:
  limits:
  - type: Container
    default:
      cpu: "2000m"
      memory: "4Gi"
    defaultRequest:
      cpu: "1000m"
      memory: "2Gi"
    max:
      cpu: "4000m"
      memory: "8Gi"
    min:
      cpu: "500m"
      memory: "1Gi"
```

## Security

### Network Policies

Restrict network access:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: felix-broker-netpol
  namespace: felix
spec:
  podSelector:
    matchLabels:
      app: felix-broker
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: felix-clients
    ports:
    - protocol: UDP
      port: 5000
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: felix-controlplane
    ports:
    - protocol: TCP
      port: 8443
  - to:
    - podSelector: {}
```

### Pod Security Standards

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: felix
  labels:
    pod-security.kubernetes.io/enforce: baseline
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/warn: restricted
```

### Security Context

```yaml
spec:
  securityContext:
    runAsNonRoot: true
    runAsUser: 10001
    fsGroup: 10001
    seccompProfile:
      type: RuntimeDefault
  containers:
  - name: broker
    securityContext:
      allowPrivilegeEscalation: false
      readOnlyRootFilesystem: true
      capabilities:
        drop:
        - ALL
```

## Operations

### Scaling

```bash
# Scale StatefulSet
kubectl scale statefulset felix-broker --replicas=5 -n felix

# Check scaling progress
kubectl rollout status statefulset/felix-broker -n felix
```

### Rolling Updates

```bash
# Update image
kubectl set image statefulset/felix-broker \
  broker=felix/broker:v0.2.0 -n felix

# Watch rollout
kubectl rollout status statefulset/felix-broker -n felix

# Rollback if needed
kubectl rollout undo statefulset/felix-broker -n felix
```

### Debugging

```bash
# View logs
kubectl logs -f felix-broker-0 -n felix

# Shell into pod
kubectl exec -it felix-broker-0 -n felix -- /bin/sh

# Port forward for local access
kubectl port-forward felix-broker-0 5000:5000 8080:8080 -n felix

# Check events
kubectl get events -n felix --sort-by='.lastTimestamp'

# Describe pod
kubectl describe pod felix-broker-0 -n felix
```

### Backup and Recovery

```bash
# Backup PVC data
kubectl exec felix-broker-0 -n felix -- tar czf - /data > backup.tar.gz

# List PVCs
kubectl get pvc -n felix

# Restore from backup
cat backup.tar.gz | kubectl exec -i felix-broker-0 -n felix -- tar xzf - -C /
```

## Complete Production Example

Full production-ready manifest:

```bash
# Clone repository
git clone https://github.com/gabloe/felix.git
cd felix/deploy/kubernetes

# Review and customize
$EDITOR production/values.yaml

# Apply with kustomize
kubectl apply -k production/

# Or use Helm (when available)
helm install felix ./charts/felix -n felix --create-namespace
```

## Troubleshooting

### Pod Stuck in Pending

```bash
kubectl describe pod felix-broker-0 -n felix
# Check for: insufficient resources, PVC binding, node affinity
```

### CrashLoopBackOff

```bash
kubectl logs felix-broker-0 -n felix --previous
# Check for: config errors, port conflicts, resource limits
```

### Service Not Reachable

```bash
# Test from within cluster
kubectl run -it --rm debug --image=busybox --restart=Never -n felix -- sh
nc -zvu felix-broker-headless 5000
```

### High Memory Usage

```bash
# Check metrics
kubectl top pods -n felix

# Adjust resource limits
kubectl set resources statefulset felix-broker \
  --limits=memory=8Gi -n felix
```

## Next Steps

- **Monitor deployment**: [Observability Guide](../features/observability.md)
- **Tune performance**: [Performance Guide](../features/performance.md)
- **Configure fully**: [Configuration Reference](../reference/configuration.md)
- **Secure deployment**: [Security Guide](../features/security.md)
