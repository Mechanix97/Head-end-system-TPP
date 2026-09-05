# Head-end-system-TPP

## How to run it
```bash
make run ## runs the HES with no metrics

make run-docker-metrics ## runs the HES in a docker with metics

make stop-docker-metrics ## stops the HES running in docker

make run-presentation ## demo stack in docker: single node, in-memory DB, metrics, test mode (frequent sessions), no RPC

make logs-presentation ## follows the demo stack logs

make stop-presentation ## stops and removes the demo stack

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

## Useful AT commands for Quectel BG95-M3
| Command | Description | Example and Response |
|---------|-------------|----------------------|
| `AT` | Checks UART communication with the module. | `AT`<br>`OK` |
| `AT+CPIN?` | Checks the SIM status. | `AT+CPIN?`<br>`+CPIN: READY`<br>`OK` |
| `AT+CPIN="xxxx"` | Enters the PIN if the SIM is locked. | `AT+CPIN="1234"`<br>`OK` |
| `AT+CSQ` | Shows signal quality (RSSI). | `AT+CSQ`<br>`+CSQ: 20,99`<br>`OK` (RSSI >10 is acceptable) |
