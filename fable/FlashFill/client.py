from xmlrpc.client import ServerProxy
import pandas as pd
import pickle
import os

proxy = ServerProxy('http://x.x.x.x:xxxx', allow_none=True)
files = ['sheet1.csv', 'sheet2.csv']

inputs = []
# Initialize for Input format: [pickled string of csv dict]
# each dict is in the format of {'sheet_name': str, 'csv': dict}
# Output column that want to flashfill to infer on must be in the name of "Output"
for path in files:
    # csv = pd.read_csv(path).to_dict(orient='list')
    csv = pd.read_csv(path)
    d = {
        'sheet_name': os.path.splitext(path)[0].capitalize(),
        'csv': csv
    }
    inputs.append(pickle.dumps(d))

outputs = proxy.handle(inputs, 'AHA_' + str(os.getpid()))
outputs = pickle.loads(outputs.data)
print([o['sheet_name'] for o in outputs])
print([pd.DataFrame(o['csv']) for o in outputs])