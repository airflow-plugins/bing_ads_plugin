# Airflow Plugin - Bing Ads
This plugin pulls data from the Bing Ads API and generates reports from the BingAds Reporting API.

## Hooks
### BingAdsClientHook
This hook handles the authentication and request to Bing Ads and allows you to interact with the Bing Ads APIs.

## Operators
### BingAdsReportOperator
This operator composes the logic for this plugin. It pulls reports from the BingAds Reporting API.
