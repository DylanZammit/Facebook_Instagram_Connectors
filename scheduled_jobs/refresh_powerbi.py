from powerbi.client import PowerBiClient
from powerbi.session import PowerBiSession
import os
import time
import pandas as pd
from logger import mylogger, pb
from traceback import format_exc
from datetime import datetime, timedelta


BASE_PATH = os.path.dirname(os.path.abspath(__file__))
logger = mylogger('refresh_powerbi/refresh')

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

        credentials = os.path.join(BASE_PATH, credentials)
        self.group = group
        self.dataflow = dataflow

        logger.info(credentials)

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
        logger.info(transaction_state)
        status = transaction_state['value'][0]['status']
        logger.info(f'Dataflow status: {status}')
        return status

    def dataset_status(self, dataset):
        transaction_state = self.pbs.make_request('get',f'myorg/groups/{self.group}/datasets/{dataset}/refreshes?$top=1')
        status = transaction_state['value']
        status = status[0]['status'] if len(status) == 9 else 'boq'
        logger.info(f'Dataset status: {status}')
        return status

    def wait_dataflow_refresh(self, raise_on_fail=False):
        while self.dataflow_status() == 'InProgress':
            logger.info('Dataflow refresh in progress. Trying again in 5s...')
            time.sleep(5)

        if raise_on_fail and self.dataflow_status() == 'Failed':
            raise ValueError(f'dataflow={self.dataflow} refresh failed')
        else:
            logger.info('Dataflow ready for refresh')

    def refresh(self, delay:int=120, until:str='2030-01-01', once:bool=False):
        until = pd.Timestamp(until)
        while datetime.now() < until:
            try:
                self.wait_dataflow_refresh()
                df_status = self.dataflow_status()
                if df_status == 'Success':
                    self.refresh_dataflow()
                    time.sleep(2)
                self.wait_dataflow_refresh(True)

                dataset_refreshed = {dataset: 0 for dataset in self.datasets}
                while not all(dataset_refreshed.values()):
                    for dataset in self.datasets:
                        if dataset_refreshed[dataset] == 0 and self.dataset_status(dataset) != 'Unknown':
                            res = self.refresh_dataset(dataset)
                            dataset_refreshed[dataset] = res
                            time.sleep(0.1)
                    time.sleep(10)
            except Exception as e:
                logger.critical('error:' + format_exc())
            logger.info(f'{datetime.now()}: Sleeping {delay}s before next refresh')
            if once: break
            time.sleep(delay)
    
    def refresh_dataflow(self):
        try:
            self.pbs.make_request('post',f'myorg/groups/{self.group}/dataflows/{self.dataflow}/refreshes')
            logger.info('dataflow Refresh started')
            return 1
        except Exception as e:
            logger.critical('error:' + format_exc())
            return 0

    def refresh_dataset(self, dataset):
        try:
            self.pbs.make_request('post', f'myorg/groups/{self.group}/datasets/{dataset}/refreshes')
            logger.info(f'Refresh dataset={dataset} request sent!')
            return 1
        except Exception as e:
            logger.critical('error:' + format_exc())
            return 0


if __name__ == '__main__':
    import yaml
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='location of config file', type=str, required=True)
    parser.add_argument('--name', help='name of group of dataflows/datasets you want to run as specified in yaml', type=str, required=True)
    parser.add_argument('--delay', help='delay between next refresh in seconds', type=int, default=60)
    parser.add_argument('--until', help='when should refresh stop', type=str, default='2030-01-01')
    parser.add_argument('--once', help='refresh once', action='store_true')
    args = parser.parse_args()

    with open(os.path.join(BASE_PATH, args.config)) as f:
        kwargs = yaml.safe_load(f)
        kwargs.update(kwargs.pop(args.name))

    Refresh(**kwargs).refresh(delay=args.delay, until=args.until, once=args.once)


