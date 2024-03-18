import requests
import pandas as pd
import jsonlines
from collections import defaultdict

"""
Data service for fetching data via Trading Stragegy api. 
Can be called for viewing available blockchains, decentralized exchanges, trading pairs and OHLCV candle data.


    Parameters:
        api_url (str): endpoint api for Trading Strategy

    Methods:
        get_chain_data():
            returns names and slugs available blockchains supported by Trading Strategy API

        get_exchange_data(selected_chain, filter_zero_volume):
            returns decentralized exchange slugs and 30-day USD volumes

        get_pairs_data(exc_slugs, chain_slugs, n_pairs, sort, filter)
            returns pair slugs, exchanges, daily USD volume and total value locked (TVL) for queried pairs

        get_ohlcv_candles(pair_ids, start_time, end_time,  time_bucket, max_bytes)
            returns OHLCV candle time series data based on inputs. Available parameter specifications from https://tradingstrategy.ai/api/explorer/#/Trading%20pair/web_candles_jsonl

"""


class DataService():

    """
    Init class with endpoint api
    """
    def __init__(self, api_url = "https://tradingstrategy.ai/api/"):
        self.api_url: str = api_url

    """
    Return all available blockchains
    """

    def get_chain_data(self):
        chains = requests.get(self.api_url + "chains")
        chain_names = [chain["chain_name"] for chain in chains.json()]
        chain_slugs = [chain["chain_slug"] for chain in chains.json()]
        chain_id = [chain["chain_id"] for chain in chains.json()]
        chain_data = pd.DataFrame(data={
            "name" : chain_names,
            "slug": chain_slugs},
            index=chain_id)
        return chain_data
    
    """
    Return all decentralized exchanges on specified blockchain, and with option to filter zero volume exchanges out

    Parameters:
        selected_chain: str
            chain slug, e.g., 'ethereum'

        filter_zero_volume: str = 'true'
            'true' for filtering inactive exchanges or 'false' for all exchanges 
    """


    def get_exchange_data(self, selected_chain: str, filter_zero_volume = "true"):
        api_extension = f"exchanges?chain_slug={selected_chain}&sort=usd_volume_30d&direction=desc&filter_zero_volume={filter_zero_volume}"
        exchanges = requests.get(self.api_url + api_extension)
        exc_names = [exc["exchange_slug"]for exc in exchanges.json()["exchanges"]]
        exc_vol = [exc["usd_volume_30d"]for exc in exchanges.json()["exchanges"]]
        exc_id = [exc["exchange_id"]for exc in exchanges.json()["exchanges"]]
        exchange_data = pd.DataFrame(data={
            "slug" : exc_names,
            "volume": exc_vol},
            index=exc_id)
        return exchange_data
    
    """
    Return pairs data based on exchange, blockchain, number of pairs, sort and filtering 

    Parameters:
        exc_slugs: list[str]
            exchange slugs, e.g., ['uniswap-v3', 'sushi']

        chain_slugs: list[str]
            chain slugs e.g., ['ethereum', 'arbitrum']

        n_pairs: int
            number of pairs fetched

        sort: str
            sorting logic, with default of 30 day USD volume

        filter: str
            filtering of pairs, with default of minimum liquidity of 1M USD. Use 'unfiltered' for no filtering 

    """

    
    def get_pairs_data(self, exc_slugs: list[str], chain_slugs: list[str], n_pairs: int = 5, sort="volume_30d", filter="min_liquidity_1M"):
        exc_slug_string = ",".join(exc_slugs)
        chain_slug_string = ",".join(chain_slugs)
        api_extension = f"pairs?exchange_slugs={exc_slug_string}&chain_slugs={chain_slug_string}&page=0&page_size={n_pairs}&sort={sort}&direction=desc&filter={filter}&eligible_only=true&format=json"
        pairs = requests.get(self.api_url + api_extension)
        pair_id = [pair["pair_id"] for pair in pairs.json()["results"]]
        pair_slug = [pair["pair_slug"] for pair in pairs.json()["results"]]
        pair_exc = [pair["exchange_slug"] for pair in pairs.json()["results"]]
        pair_vol = [pair["usd_volume_24h"] for pair in pairs.json()["results"]]
        pair_tvl =  [pair["pair_tvl"] for pair in pairs.json()["results"]]
        pair_data = pd.DataFrame(data={
            "pair_slug" : pair_slug,
            "pair_exchange": pair_exc,
            "usd_vol_24h":pair_vol,
            "pair_tvl": pair_tvl}, 
            index=pair_id)
        return pair_data
    
    """
    Return Open High Low Close Volume (OHLCV) candle data with set pairs, start and end times and frequence

    Parameters:
        pair_ids: list[int|str]:
            pairs by Trading Strategy specified ID in either list of string or int form
        start_time: str 
            UNIX UTC timestamp, e.g., '2024-01-01'
        
        end_time: str
            UNIX UTC timestamp, e.g., '2024-01-01'

        time_bucket: str
            set frequency of data, with default of 15m (15 minutes). Check available formats from https://tradingstrategy.ai/api/explorer/#/Trading%20pair/web_candles_jsonl

        max_bytes: int
            set a limit of return data size in bytes. Default of 250 megabytes

    """

    
    def get_ohlcv_candles(self, pair_ids: list[int|str], start_time: str, end_time: str,  time_bucket="15m", max_bytes = 250000000):
        pair_ids_string = ",".join(map(str, pair_ids))
        api_extension = f"candles-jsonl?pair_ids={pair_ids_string}&time_bucket={time_bucket}&start={start_time}&end={end_time}&max_bytes={max_bytes}"
        response = requests.get(self.api_url + api_extension, stream=True)

        reader = jsonlines.Reader(response.raw)
        candle_dict = defaultdict(list)
        for item in reader:
            pair_id = item["p"]
            candle_dict[pair_id].append(item)

        return_dict = {}

        for key, value in candle_dict.items():
            ts = [val["ts"] for val in value]
            open = [val["o"] for val in value]
            high = [val["h"] for val in value]
            low = [val["l"] for val in value]
            close = [val["c"] for val in value]
            volume = [val["v"] for val in value]
            df = pd.DataFrame(data={"Open": open, "High": high, "Low": low, "Close": close, "Volume": volume}, index=pd.to_datetime(ts, unit="s"))
            return_dict[key] = df

        return return_dict
