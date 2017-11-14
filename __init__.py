from airflow.plugins_manager import AirflowPlugin
from bing_ads_plugins.hooks.bing_ads_client_v11_hook import BingAdsHook
from bing_ads_plugins.operators.bing_ads_report_operator import BingAdsOperator


class BingAdsPlugin(AirflowPlugin):
    name = "bing_ads_plugin"
    operators = [BingAdsOperator]
    hooks = [BingAdsHook]
