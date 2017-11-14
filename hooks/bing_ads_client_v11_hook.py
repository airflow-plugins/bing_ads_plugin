"""Bing Ads Client Hook"""
from bingads.service_client import ServiceClient
from bingads.authorization import *
from bingads.v11 import *
from bingads.v11.bulk import *
from bingads.v11.reporting import *

from airflow.hooks.base_hook import BaseHook
from airflow.models import Connection
from airflow.utils.db import provide_session

import json
import logging

logging.getLogger('suds').setLevel(logging.CRITICAL)


class BingAdsHook(BaseHook):
    """
    Interact with Bing Ads Apis
    """

    def __init__(self, config, file_name, path, start_date, end_date,
                 conn_id="", *args, **kwargs):
        """
        Args:

        """

        self.start_date = start_date
        self.end_date = end_date
        self.conn_id = conn_id
        self.ENVIRONMENT = "production"
        self.VERSION = 11
        self.CLIENT_ID = config.CLIENT_ID
        self.CLIENT_STATE = config.CLIENT_STATE
        self.TIMEOUT_IN_MILLISECONDS = 36000000
        self.FILE_DIRECTORY = path
        self.file_name = file_name

    def runReport(self, auth_data, reportReq, rep_ser, rep_ser_man):

        global authorization_data, reporting_service, reporting_service_manager

        authorization_data = auth_data
        reporting_service = rep_ser
        reporting_service_manager = rep_ser_man

        self.authenticate(authorization_data)
        self.getReport(authorization_data, reportReq)

    def authenticate(self, authorization_data):
        """
        Args:
        """
        # You should authenticate for Bing Ads production services with a Microsoft Account,
        # instead of providing the Bing Ads username and password set.
        # Authentication with a Microsoft Account is currently not supported in Sandbox.
        self.authenticate_with_oauth(authorization_data)

    def authenticate_with_oauth(self, authorization_data):

        authentication = OAuthDesktopMobileAuthCodeGrant(
            client_id=self.CLIENT_ID
        )

        # It is recommended that you specify a non guessable 'state' request parameter to help prevent
        # cross site request forgery (CSRF).
        authentication.state = self.CLIENT_STATE

        # Assign this authentication instance to the authorization_data.
        authorization_data.authentication = authentication

        # Register the callback function to automatically save the refresh token anytime it is refreshed.
        # Uncomment this line if you want to store your refresh token. Be sure to save your refresh token securely.
        authorization_data.authentication.token_refreshed_callback = self.save_refresh_token

        refresh_token = self.get_refresh_token()

        try:
            # If we have a refresh token let's refresh it
            if refresh_token is not None:
                authorization_data.authentication.request_oauth_tokens_by_refresh_token(
                    refresh_token)
            else:
                raise OAuthTokenRequestException(
                    "error getting refresh token", "Refresh token needs to be requested manually.")
        except OAuthTokenRequestException:
            # The user could not be authenticated or the grant is expired.
            # The user must first sign in and if needed grant the client application access to the requested scope.
            raise Exception(
                "Refresh token needs to be requested. Reference Docs.")

    def get_refresh_token(self):
        """
        Args:
        """
        conn = self.get_connection(self.conn_id)
        extra = json.loads(conn.extra)
        return extra['refresh_token']

    # TODO: Impliment save_refresh_token
    @provide_session
    def save_refresh_token(self, oauth, session=None):
        '''
        Stores a refresh token locally. Be sure to save your refresh token securely.
        '''
        print("this is oauth.refresh_token", oauth.refresh_token)
        ba_conn = session.query(Connection).filter(
            Connection.conn_id == self.conn_id).first()
        ba_conn.extra = json.dumps({"refresh_token": oauth.refresh_token})
        session.commit()

    def getReport(self, authorization_data, reportReq):

        try:
            report_request = reportReq

            report_time = reporting_service.factory.create('ReportTime')
            custom_date_range_start = reporting_service.factory.create('Date')
            custom_date_range_start.Day = self.start_date.day
            custom_date_range_start.Month = self.start_date.month
            custom_date_range_start.Year = self.start_date.year
            report_time.CustomDateRangeStart = custom_date_range_start
            custom_date_range_end = reporting_service.factory.create('Date')
            custom_date_range_end.Day = self.end_date.day
            custom_date_range_end.Month = self.end_date.month
            custom_date_range_end.Year = self.end_date.year
            report_time.CustomDateRangeEnd = custom_date_range_end
            report_time.PredefinedTime = None
            report_request.Time = report_time

            reporting_download_parameters = ReportingDownloadParameters(
                report_request=report_request,
                result_file_directory=self.FILE_DIRECTORY,
                result_file_name=self.file_name,
                # Set this value true if you want to overwrite the same file.
                overwrite_result_file=True,
                # You may optionally cancel the download after a specified time interval.
                timeout_in_milliseconds=self.TIMEOUT_IN_MILLISECONDS
            )

            # Option A - Background Completion with Rep  ortingServiceManager
            # You can submit a download request and the ReportingServiceManager will automatically
            # return results. The ReportingServiceManager abstracts the details of checking for
            # result file completion, and you don't have to write any code for results polling.

            # output_status_message("Awaiting Background Completion . . .")
            self.background_completion(reporting_download_parameters)

            # Option B - Submit and Download with ReportingServiceManager
            # Submit the download request and then use the ReportingDownloadOperation result to
            # track status yourself using ReportingServiceManager.get_status().

            # output_status_message("Awaiting Submit and Download . . .")
            self.submit_and_download(report_request, self.file_name)

            # Option C - Download Results with ReportingServiceManager
            # If for any reason you have to resume from a previous application state,
            # you can use an existing download request identifier and use it
            # to download the result file.

            # For example you might have previously retrieved a request ID using submit_download.
            reporting_operation = reporting_service_manager.submit_download(
                report_request)
            request_id = reporting_operation.request_id

            # Given the request ID above, you can resume the workflow and download the report.
            # The report request identifier is valid for two days.
            # If you do not download the report within two days, you must request the report again.
            # output_status_message("Awaiting Download Results . . .")
            self.download_results(request_id, authorization_data,
                                  self.file_name)

            # output_status_message("Program execution completed")

        except Exception as ex:
            print(ex)
            # output_status_message(ex)

    def background_completion(self, reporting_download_parameters):
        '''
        You can submit a download request and the ReportingServiceManager will automatically
        return results. The ReportingServiceManager abstracts the details of checking for result file
        completion, and you don't have to write any code for results polling.
        '''
        global reporting_service_manager
        result_file_path = reporting_service_manager.download_file(
            reporting_download_parameters)
        # output_status_message(
        #    "Download result file: {0}\n".format(result_file_path))

    def submit_and_download(self, report_request, download_file_name):
        '''
        Submit the download request and then use the ReportingDownloadOperation result to
        track status until the report is complete e.g. either using
        ReportingDownloadOperation.track() or ReportingDownloadOperation.get_status().
        '''
        global reporting_service_manager
        reporting_download_operation = reporting_service_manager.submit_download(
            report_request)

        # You may optionally cancel the track() operation after a specified time interval.
        reporting_operation_status = reporting_download_operation.track(
            timeout_in_milliseconds=self.TIMEOUT_IN_MILLISECONDS)

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory=self.FILE_DIRECTORY,
            result_file_name=download_file_name,
            decompress=True,
            # Set this value true if you want to overwrite the same file.
            overwrite=True,
            # You may optionally cancel the download after a specified time interval.
            timeout_in_milliseconds=self.TIMEOUT_IN_MILLISECONDS
        )

        # output_status_message("Download result file: {0}\n".format(result_file_path))

    def download_results(self, request_id, authorization_data, download_file_name):
        '''
        If for any reason you have to resume from a previous application state,
        you can use an existing download request identifier and use it
        to download the result file. Use ReportingDownloadOperation.track() to indicate that the application
        should wait to ensure that the download status is completed.
        '''
        reporting_download_operation = ReportingDownloadOperation(
            request_id=request_id,
            authorization_data=authorization_data,
            poll_interval_in_milliseconds=1000,
            environment='production',
        )

        # Use track() to indicate that the application should wait to ensure that
        # the download status is completed.
        # You may optionally cancel the track() operation after a specified time interval.
        reporting_operation_status = reporting_download_operation.track(
            timeout_in_milliseconds=self.TIMEOUT_IN_MILLISECONDS)

        result_file_path = reporting_download_operation.download_result_file(
            result_file_directory=self.FILE_DIRECTORY,
            result_file_name=download_file_name,
            decompress=True,
            # Set this value true if you want to overwrite the same file.
            overwrite=True,
            # You may optionally cancel the download after a specified time interval.
            timeout_in_milliseconds=self.TIMEOUT_IN_MILLISECONDS
        )

        # output_status_message("Download result file: {0}".format(result_file_path))
        # output_status_message("Status: {0}\n".format(reporting_operation_status.status))
