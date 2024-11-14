import xlwings as xw
import string
import json, csv
import os
import pandas as pd
import pickle
from contextlib import contextmanager
import threading
import _thread

class TimeoutException(Exception):
    def __init__(self, msg=''):
        self.msg = msg

@contextmanager
def time_limit(seconds, msg=''):
    timer = threading.Timer(seconds, lambda: _thread.interrupt_main())
    timer.start()
    try:
        yield
    except KeyboardInterrupt:
        raise TimeoutException("Timed out for operation {}".format(msg))
    finally:
        # if the action ends in specified time, timer is canceled
        timer.cancel()

def _to_xlsx_idx(n):
    s = ''
    n += 1
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        s = string.ascii_uppercase[remainder] + s
    return s

class FlashFillHandler:
    def handle(self, inputs, identifier):
        """
        csvs: list of pickled dict with {'sheet_name': str, 'csv': dict for flashfill
        # TODO: Consider using protobuf for different language support
        """
        inputs = [pickle.loads(i.data) for i in inputs]
        csvs = [i['csv'] for i in inputs]
        sheet_names = [i['sheet_name'] for i in inputs]
        xlsx_path, output_cols = self.csv_xlsx(csvs, sheet_names, identifier)
        self.fill(xlsx_path, output_cols)
        outputs = self.xlsx_csv(xlsx_path)
        return pickle.dumps(outputs)
        
    def fill(self, xlsx_path, output_cols, visible=False):
        """
        xlsx can have multiple sheet for higher throughput
        output_col: list(list), len=length of sheet. The #col for flashfill to be run
        """
        wb = xw.Book(xlsx_path)
        try:
            with time_limit(20):
                for ws in wb.sheets: 
                    cols = output_cols[ws.name]
                    # assert(cols[-1] < 26)
                    for col in cols:
                        idx = _to_xlsx_idx(col)
                        try:
                            r = ws.range(f'{idx}1')
                            r.api.FlashFill()
                        except Exception as e:
                            print('Flashfill:', str(e))
            wb.save()
            wb.close()
        except:
            wb.close() 
            print('Flashfill: Timeout')

    def csv_xlsx(self, csvs, sheet_names, identifier, output_name='Output'):
        """
        Merge received csvs into an xlsx with multiple sheets
        Reorder the Output to the end of the Column
        csvs: dict representing csv

        return: xlsx_path, output_cols
        """
        self.app = xw.App(visible=False)
        wb = xw.Book()
        # csvs = [pd.DataFrame(csv) for csv in csvs]
        output_cols, self.headers = {}, {}
        for name, df in zip(sheet_names, csvs):
            cols = df.columns.tolist()
            # assert(output_name in cols)
            input_col = [c for c in cols if output_name not in c]
            output_col = [c for c in cols if output_name in c]
            cols = input_col + output_col
            self.headers[name] = cols
            df = df[cols]
            output_cols[name] = list(range(len(input_col), len(cols)))
            row, col = df.shape
            try:
                wb.sheets.add(name)
            except: pass
            sheet = wb.sheets[name]
            for i in range(row):
                for j in range(col):
                    range_str = _to_xlsx_idx(j) + str(i+1)
                    sheet.range(range_str).number_format = '@'
                    sheet.range(range_str).value = df.iloc[i, j]
        wb.save(f'output\\{identifier}.xlsx')
        wb.close()
        return f"output\\{identifier}.xlsx", output_cols

    def xlsx_csv(self, xlsx_path):
        """
        Split different sheets in xlsx into different csvs --> dict

        returns: Same as input of csv_xlsx
        """ 
        self.app.kill()
        outputs = []
        with open(xlsx_path, 'rb') as xlsx_file:
            excel = pd.read_excel(xlsx_file, header=None, sheet_name=None, engine='openpyxl', dtype=str)
            for sheet_name, csv in excel.items():
                csv.columns = self.headers[sheet_name]
                outputs.append({
                    'sheet_name': sheet_name,
                    # 'csv': csv.to_dict(orient='list')
                    'csv': csv
                })
        os.remove(xlsx_path)
        return outputs