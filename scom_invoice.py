import pandas as pd
import tabula
import requests
from dataclasses import dataclass
from typing import List, Dict
import PyPDF2
import re
from datetime import datetime
import os


@dataclass
class ScomServer:
    id: int
    name: str
    currency: str
    acc: str
    ipv4: str = "127.0.0.1"
    price_monthly: float = 0.0
    invoice_date: str = "2008-01-01"
    backup: int = 0


class InvoiceProcessor:
    def __init__(self, downloaded_pdf_files: List[str], tokens: Dict[str, str]):
        self.downloaded_pdf_files = downloaded_pdf_files
        self.tokens = tokens
        self.merged_df, self.df_per_acc = self.__load_and_dataframes()
        self.invoice_date, self.invoice_currency, self.invoice_numbers = self.__get_date_and_currency()
        self.servers = {}
        self.racks = {}
        self.lb = {}
        self.BASE_URL = "https://api.servers.com/v1"
        self.headers = {
            "Content-Type": "application/json"
        }

    def __load_and_dataframes(self):
        all_dfs = []
        df_per_acc = {}
        buffer_list = []
        for file in self.downloaded_pdf_files:
            acc = file.split("/")[0]
            buffer_list_per_acc = []
            # dfs will contain list of pages fron one file
            dfs = tabula.read_pdf(file, pages='all')

            for df in dfs:
                # if pdf has parsed incorrectly, we fix it. PDF from different accounts has different structure.
                df = self.fix_headers(df)
                buffer_list.append(df)
                buffer_list_per_acc.append(df)

            df_per_acc[acc] = pd.concat(buffer_list_per_acc, ignore_index=True)

        all_dfs = pd.concat(buffer_list, ignore_index=True)

        return all_dfs, df_per_acc

    def __convert_date(self, date_string):
        date_string = date_string.strip()  # Remove any trailing or leading whitespace
        date_object = datetime.strptime(date_string, '%B %d, %Y')
        return date_object.strftime('%Y-%m-%d')

    def __get_date_and_currency(self):
        date_dict = {}
        currency_dict = {}
        invoice_numbers = {}
        for file_path in self.downloaded_pdf_files:
            acc = file_path.split("/")[0]
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ''
                for page in pdf_reader.pages:
                    text += page.extract_text()
                # Look for the pattern
                date_pattern = re.compile(r'Due\sdate:\s(.*)')
                currency_pattern = re.compile(r'(\w+)\s+Currency')
                invoice_no = re.compile(r'No:\s(.*)')
                date_match = date_pattern.search(text)
                cur_match = currency_pattern.search(text)
                num = invoice_no.search(text)
                if cur_match:
                    if "DOLLAR" in cur_match.group(1) or "USD" in cur_match.group(1):
                        currency_dict[acc] = "Dollar"
                    else:
                        currency_dict[acc] = "Euro"
                # Some times it can't parse currency from pdf file. So we hardcode Euro as default case
                # Until we come to better idea
                else:
                    currency_dict[acc] = "Euro"
                if date_match:
                    date_dict[acc] = self.__convert_date(date_match.group(1))
                if num:
                    invoice_numbers[acc] = num.group(1)

        return date_dict, currency_dict, invoice_numbers


    @staticmethod
    def fix_headers(df: pd.DataFrame):
        if 'Original' in df.columns:
            df = df.drop(columns=['Original'])
        if 'Description' not in df.columns:
            df.columns = df.iloc[0]
            df = df[1:]

        return df


    @staticmethod
    def end_parsing(row, end_entries):
        if pd.isna(row):
            return False
        return any(end_entry in row for end_entry in end_entries)


    def _get_cloud_storage_price(self, account):
        end_entries = ['Hosting:', 'L2', 'Private rack:', 'Cloud Computing']
        return self.get_prices(account=account, start_entry="Cloud Storage", end_entries=end_entries)

    def _get_cloud_computing_price(self, account):
        end_entries = ['Hosting:', 'Cloud Storage', 'L2', 'Private rack:', "Load Balancer:"]
        return self.get_prices(account=account, start_entry="Cloud Computing", end_entries=end_entries)

    def _get_rack_price(self, account):
        end_entries = ['Hosting:', 'Cloud Storage', 'L2', 'Cloud Computing', "Load Balancer:"]
        return self.get_prices(account=account, start_entry="Private rack:", end_entries=end_entries)

    def _get_lb_price(self, account):
        end_entries = ['Hosting:', 'Cloud Storage', 'L2', 'Private rack:', 'Cloud Computing']
        return self.get_prices(account=account, start_entry="Load Balancer:", end_entries=end_entries)

    def _get_free_servers_price(self, account):
        end_entries = ['Hosting:', 'Cloud Storage', 'L2', 'Private rack:', 'Cloud Computing', "Load Balancer:"]
        return self.get_reserv_serv_prices(account=account, start_entry="free", end_entries=end_entries)

    def _get_server_price(self, serv):
        end_entries = ['Hosting:', 'Cloud Storage', 'L2', 'Private rack:', 'Cloud Computing', "Load Balancer:"]
        return self.get_prices(start_entry=serv, end_entries=end_entries)

    def _get_l2_segment_price(self, account):
        end_entries = ['Hosting:', 'Cloud Storage', 'Private rack:', 'Cloud Computing', "Load Balancer:"]
        return self.get_prices(account=account, start_entry="L2 Segment:", end_entries=end_entries)

    def get_reserv_serv_prices(self, account=None, start_entry='', end_entries=''):
        capture = False
        subtotal = 0
        currency = ""

        for _, row in self.df_per_acc[account].iterrows():
            if pd.notna(row['Description']):
                if start_entry in row['Description']:
                    capture = True
                elif self.end_parsing(row['Description'], end_entries):
                    capture = False

            if capture and pd.notna(row['Subtotal']):
                price = row['Subtotal'].replace('€', '').replace('$', '')
                subtotal += float(price)
                currency = 'Euro' if '€' in row['Subtotal'] else 'Dollar'

        return {"price_monthly": round(subtotal, 2), "currency": currency}

    def get_prices(self, account=None, start_entry='', end_entries=''):
        subtotal = 0
        currency = ""
        pattern = '|'.join(end_entries)

        if account:
            search_string = rf'{start_entry}'
            df = self.df_per_acc[account]
            self.invoice_currency[account]
        else:
            search_string = rf'(?<![\w-]){start_entry}(?![\w-])'
            df = self.merged_df

        descriptions = df['Description'].fillna('')
        start_mask = descriptions.str.contains(search_string, case=False, regex=True).fillna(False)

        if start_mask.sum() > 0:
            start_idx = start_mask.idxmax()
            slice_df = df.iloc[start_idx:]
            slice_df_description = slice_df['Description'].fillna('')
            end_mask = slice_df_description.str.contains(pattern, case=False, regex=True)

            if end_mask.sum() > 0:
                end_idx = end_mask.idxmax()
            else:
                # If no end_mask is found, set the end_idx to the last index of the dataframe
                end_idx = df.index[-1]

            rows_of_interest = df.iloc[start_idx:end_idx+1]
            subtotal_col = rows_of_interest['Subtotal'].fillna("€0.0")
            subtotal_row = pd.to_numeric(subtotal_col.str.replace("€", "").astype(float))
            subtotal = subtotal_row.sum()

        return {"price_monthly": round(subtotal, 2), "currency": currency}

    def __make_api_request(self, endpoint, api_key):
        num = 1
        self.headers["Authorization"] = f"Bearer {api_key}"
        all_servers = []
        next_page = True

        while next_page:
            url = f"{self.BASE_URL}/{endpoint}?page={num}"
            response = requests.get(url, headers=self.headers)
            response_json = response.json()

            if response_json:
                servers = response_json
                all_servers.extend(servers)
                num += 1
            else:
                next_page = False

        return all_servers

    def __process_server_dict(self, servers_dict, account):
        processed_serv_dict = {}

        for val in servers_dict:
            invoice_price = self._get_server_price(str(val['title']))

            serv = ScomServer(
                id=val['id'],
                name=val['title'],
                ipv4=str(val['public_ipv4_address']),
                price_monthly=invoice_price["price_monthly"],
                acc=account,
                currency=self.invoice_currency[account],
                invoice_date=self.invoice_date[account]
            )
            processed_serv_dict[str(val['public_ipv4_address'])] = serv

        return processed_serv_dict

    def __process_vm(self, processed_cloud_compute, api_key, account):
        vm_list = self.__make_api_request("cloud_computing/instances", api_key)
        vm_dict = {}
        snap_filtered_df = self.df_per_acc[account].loc[self.df_per_acc[account]['Description'].str.contains('Cloud Snapshots', na=False)]
        snap_subtotal = 0
        backup_count = 0
        backuped_vm = {}
        not_backuped_vm = {}
        snap_price_per_vm = 0
        solo_vm_price = 0
        vm_count = 0
        if snap_filtered_df.empty:
            snap_filtered_df = {}
            snap_filtered_df['Subtotal'] = ['€0.0']

        for price in snap_filtered_df['Subtotal']:
            p = price.replace('€', '').replace('$', '')
            snap_subtotal += float(p)
        vm_subtotal = processed_cloud_compute['price_monthly'] - snap_subtotal
        for vm in vm_list:
            vm_count += 1
            vm_serv = ScomServer(
                id=vm['id'],
                name=vm['name'],
                ipv4=vm['public_ipv4_address'],
                acc=account,
                price_monthly=0,
                currency=processed_cloud_compute['currency'],
                backup=vm['backup_copies'],
                invoice_date=os.environ.get('custom_date', self.invoice_date[account])
            )

            if vm['backup_copies'] > 0:
                backup_count += 1
                backuped_vm[vm['public_ipv4_address']] = vm['backup_copies']
            else:
                not_backuped_vm[vm['public_ipv4_address']] = 1

            vm_dict[vm['public_ipv4_address']] = vm_serv

        if vm_count > 0:
            solo_vm_price = vm_subtotal/vm_count

        if snap_subtotal != 0 and backup_count != 0: 
            snap_price_per_vm = snap_subtotal/backup_count

        if solo_vm_price != 0:
            for key in backuped_vm.keys():
                vm_dict[key].price_monthly = round(snap_price_per_vm + solo_vm_price, 2)
            for key in not_backuped_vm.keys():
                vm_dict[key].price_monthly = round(solo_vm_price, 2)

        return vm_dict

    def process(self):

        servers_dict = {}
        processed_serv_dict = {}
        processed_lbs = {}
        processed_cloud_compute = {}
        processed_cloud_storage = {}
        processed_racks = {}
        processed_free_servers = {}
        processed_L2_segments = {}
        result = {}
        result["extra_cost"] = {}

        for account, api_key in self.tokens.items():

            servers_dict[account] = self.__make_api_request("hosts", api_key)
            processed_serv_dict.update(self.__process_server_dict(servers_dict[account], account))
            processed_racks = self._get_rack_price(account)
            processed_lbs = self._get_lb_price(account)
            processed_cloud_storage = self._get_cloud_storage_price(account)
            processed_cloud_compute = self._get_cloud_computing_price(account)
            processed_free_servers = self._get_free_servers_price(account)
            processed_L2_segments = self._get_l2_segment_price(account)

            result["extra_cost"][account] = {
                "currency": processed_racks["currency"],
                "load_balancers": processed_lbs["price_monthly"],
                "rackspot_reserv": processed_racks["price_monthly"],
                "cloud_storage": processed_cloud_storage["price_monthly"],
                "cloud_compute": processed_cloud_compute["price_monthly"],
                "comunication_devices": processed_L2_segments["price_monthly"],
                "spare_servers": processed_free_servers["price_monthly"],
                "invoice_date": os.environ.get('custom_date', self.invoice_date[account])
            }

            processed_serv_dict.update(self.__process_vm(processed_cloud_compute, api_key, account))

        return processed_serv_dict, result
