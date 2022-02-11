# Sadedegel-UI


### Install
```
// if you don't already have pnpm installed as a package manager
npm install -g pnpm

pnpm pinstall:clean

pnpm start
```
---
### Docker
```
docker build -f Dockerfile -t sadedegel-ui .
docker run -it --rm -p 3000:3000 sadedegel-ui:latest
```

### K8S
```
docker build -t registry.172.12.2.63.nip.io/sadedegel-web:v0.1 -t registry.172.12.2.63.nip.io/sadedegel-web:latest .

docker push registry.172.12.2.63.nip.io/sadedegel-web:latest

kubectl apply -f k8s/sadedegel-web-deployment.yaml -f k8s/sadedegel-web-service.yaml -f k8s/sadedegel-web-ingress.yaml
```