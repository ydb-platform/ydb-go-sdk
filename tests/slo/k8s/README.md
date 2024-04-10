1) resize colima vm to 8-16 GB
2) colima ssh
    - sudo sysctl fs.inotify.max_user_watches=524288
    - sudo sysctl fs.inotify.max_user_instances=512
3) kind create cluster --config e2e/kind-cluster-config.yaml -n ydb
4) docker pull cr.yandex/crptqonuodf51kdj7a7d/ydb:24.1.12
5) docker pull cr.yandex/yc/ydb-kubernetes-operator:0.5.2
6) docker tag cr.yandex/yc/ydb-kubernetes-operator:0.5.2 kind/ydb-operator:current
7) docker pull k8s.gcr.io/ingress-nginx/kube-webhook-certgen:v1.0
7) kind load docker-image kind/ydb-operator:current -n ydb
8) kind load docker-image cr.yandex/crptqonuodf51kdj7a7d/ydb:24.1.12 -n ydb
9) kind load docker-image k8s.gcr.io/ingress-nginx/kube-webhook-certgen:v1.0 -n ydb
10) helm -n ydb install --wait ydb-operator deploy/ydb-operator --create-namespace -f ./operator-values.yaml
11) k apply -f ./storage-block-4-2.yaml
12) k apply -f ./database.yaml
13) telepresence helm install
13) telepresence connect --namespace ydb
14) docker run -d -p 9091:9091 prom/pushgateway