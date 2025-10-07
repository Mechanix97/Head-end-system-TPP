# Head-end-system-TPP

## How to run it
```bash
make run ## runs the HES with no metrics

make run-docker-metrics ## runs the HES in a docker with metics

make stop-docker-metrics ## stops the HES running in docker

cargo run -- --help ## displays the cli args
```

## Ports in use (default)
- 6464: metrics
- 6565: backdoor 
- 6969: grafana
- 9090: prometheus

## Grafana user & password
- user: admin
- psw: admin
