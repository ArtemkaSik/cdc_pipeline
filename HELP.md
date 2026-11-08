Остановить под:
```bash
kubectl delete statefulset mysql -n databases
```
```bash
kubectl delete pods mysql-0 -n databases
```

```bash
kubectl rollout resume statefulset mysql -n databases
```