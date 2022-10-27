from powerbi.client import PowerBiClient
from powerbi.session import PowerBiSession
import time
import pandas as pd
from traceback import format_exc
from datetime import datetime, timedelta


class Refresh:
    def __init__(
            self, 
            client_id, 
            client_secret, 
            credentials, 
            group, 
            dataflow, 
            datasets=None
        ):
        self.datasets = [] if datasets is None else datasets


        self.group = group
        self.dataflow = dataflow

        # Initialize the Client.
        power_bi_client = PowerBiClient(
            client_id=client_id,
            client_secret=client_secret,
            credentials=credentials,
            scope=['https://analysis.windows.net/powerbi/api/.default'],
            redirect_uri='https://localhost/redirect',
        )
        at = power_bi_client.power_bi_auth_client.access_token
        pbs = PowerBiSession(power_bi_client.power_bi_auth_client)
        self.pbs = pbs


    def dataflow_status(self):
        transaction_state = self.pbs.make_request('get',f'myorg/groups/{self.group}/dataflows/{self.dataflow}/transactions?$top=1')
        #print(transaction_state)
        status = transaction_state['value'][0]['status']
        print(f'Dataflow status: {status}')
        return status

    def dataset_status(self, dataset):
        transaction_state = self.pbs.make_request('get',f'myorg/groups/{self.group}/datasets/{dataset}/refreshes?$top=1')
        status = transaction_state['value']
        status = status[0]['status'] if len(status) == 9 else 'boq'
        print(f'Dataset status: {status}')
        return status

    def wait_dataflow_refresh(self):
        while self.dataflow_status() == 'InProgress':
            time.sleep(5)

        if self.dataflow_status() == 'Failed':
            raise ValueError(f'dataflow={self.dataflow} refresh failed')

    def refresh(self, delay:int=120, until:str='2030-01-01'):
        until = pd.Timestamp(until)
        while datetime.now() < until:
            try:

                self.wait_dataflow_refresh()
                df_status = self.dataflow_status()
                if df_status == 'Success':
                    self.refresh_dataflow()
                self.wait_dataflow_refresh()


                dataset_refreshed = {dataset: 0 for dataset in self.datasets}
                while not all(dataset_refreshed.values()):
                    for dataset in self.datasets:
                        if dataset_refreshed[dataset] == 0 and self.dataset_status(dataset) != 'Unknown':
                            res = self.refresh_dataset(dataset)
                            dataset_refreshed[dataset] = res
                            time.sleep(0.1)
                    time.sleep(10)

            except Exception as e:
                print('error:' + format_exc())
            print(f'{datetime.now()}: Sleeping {delay}s before next refresh')
            time.sleep(delay)
    
    def refresh_dataflow(self):
        try:
            self.pbs.make_request('post',f'myorg/groups/{self.group}/dataflows/{self.dataflow}/refreshes')
            print('dataflow Refresh started')
            return 1
        except Exception as e:
            print('error:' + format_exc())
            return 0

    def refresh_dataset(self, dataset):
        try:
            self.pbs.make_request('post', f'myorg/groups/{self.group}/datasets/{dataset}/refreshes')
            print(f'Refresh dataset={dataset} request sent!')
            return 1
        except Exception as e:
            print('error:' + format_exc())
            return 0


if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='location of config file', type=str, required=True)
    parser.add_argument('--name', help='name of group of dataflows/datasets you want to run as specified in yaml', type=str, required=True)
    parser.add_argument('--delay', help='delay between next refresh in seconds', type=int, default=60)
    parser.add_argument('--until', help='when should refresh stop', type=str, default='2030-01-01')
    args = parser.parse_args()

    with open(args.config) as f:
        kwargs = yaml.safe_load(f)
        kwargs.update(kwargs.pop(args.name))

    Refresh(**kwargs).refresh(delay=args.delay, until=args.until)

