## RPC Module for utilizing MS Excel FlashFill

### Requirements
- On Windows 10/Server OS
- MS Excel is installed
- Program run with GUI (RDP also works)
- Make sure that inbound rule of port binding is configured/opened from Windows Firewall.

```sh
pip install -r requirements.txt
```

### From server side
```sh
python .\rpc.py
```

### From client side
Follows the sample of client.py
- Argument passed format: [pickled{'sheet_name': str, 'csv': dict}]
- Output column that want to flashfill to infer on must be in the prefix of "Output"
- Return value will be same as input. UnPickled required. Results will be in the prefix of "Ouput" column

Note: Currently #cols no more than 26.