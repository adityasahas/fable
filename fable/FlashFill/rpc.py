from xmlrpc.server import SimpleXMLRPCServer
from flashfill import FlashFillHandler
import os 

if 'output' not in os.listdir('.\\'):
    os.mkdir('output')

server = SimpleXMLRPCServer(('0.0.0.0', 5678), logRequests=True, allow_none=True)
server.register_instance(FlashFillHandler())

if __name__ == '__main__':
    try:
        print('Listening')
        server.serve_forever()
    except KeyboardInterrupt:
        print("Exiting")