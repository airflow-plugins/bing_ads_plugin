"""
This Operator pulls Reports from BingAds Reporting Api.
"""
from datetime import datetime
import sys
import json
import boto
import boto.s3
from boto.s3.key import Key

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from bing_ads_plugins.hooks.bing_ads_client_v11_hook import BingAdsHook


class BingAdsOperator(BaseOperator):

    class Config(object):
        def __init__(self):
            self

    template_fields = ('params',)

    @apply_defaults
    def __init__(self, bucket_name, env, config, conn_id=None,
                 *args, **kwargs):

        super(BingAdsOperator, self).__init__(*args, **kwargs)
        self.config = config
        self.authorization_data = self.config.AUTHORIZATION_DATA
        self.report_request = self.config.reportReq
        self.reporting_service = self.config.REPORTING_SERVICE
        self.reporting_service_manager = self.config.REPORTING_SERVICE_MANAGER
        self.xcom_push = True
        self.env = env
        self.conn_id = conn_id
        self.bucket_name = bucket_name

    def execute(self, context):
        self.downloadReport(self.config)
        self.uploadFile()
        return json.dumps({'input': {'key': self.config.file}})

    def removeLines(self, file, start):
        """
        The csv that is returned from the api has a huge header of meta data that needs
        removed for downstream.
        """
        lines = open(file).readlines()
        open(file, 'w').writelines(lines[start:-1])

    def downloadReport(self, config):
        """
        Using the client hook it requests the report and then removes the header.
        """
        start_date = datetime.strptime(self.params['start'], "%Y-%m-%d")
        # Uses the start date if end date is None
        end_date = datetime.strptime(
            self.params['end'] if self.params['end']
            else self.params['start'],
            "%Y-%m-%d")
        ba_hook = BingAdsHook(conn_id=self.conn_id, start_date=start_date, end_date=end_date,
                              config=self.config, file_name=self.config.file, path=self.config.path)
        ba_hook.runReport(self.authorization_data, self.report_request,
                          self.reporting_service, self.reporting_service_manager)
        self.removeLines(config.path + config.file, 10)

    def uploadFile(self):
        """
        Similar to other aries connectors this operator uploads files to s3 at the end of execute.
        """
        conn = boto.connect_s3(
            self.env['AWS_ACCESS_KEY_ID'], self.env['AWS_SECRET_ACCESS_KEY'])

        bucket = conn.get_bucket(self.bucket_name)

        print('Uploading %s to Amazon S3 bucket %s' %
              (self.config.file, self.bucket_name))

        def percent_cb(complete, total):
            sys.stdout.write('.')
            sys.stdout.flush()

        k = Key(bucket)
        k.key = self.config.file
        k.set_contents_from_filename(self.config.path + self.config.file,
                                     cb=percent_cb, num_cb=10)
