import logging
import urllib

import requests
from config import Config
from requests_ntlm import HttpNtlmAuth


class BookingData:
    def __init__(self, destination, date_from, date_to):
        self.ssrs_url = Config.SSRS_BASE_URL + destination + " Reports/Reservations/Bookings Data"
        self.ssrs_usr = Config.SSRS_USERNAME
        self.ssrs_pwd = Config.SSRS_PASSWORD
        self.payload = [
            ("from", date_from),
            ("to", date_to),
            ("d1:isnull", True),
            ("d2:isnull", True),
            ("MaxProcessDate_from:isnull", True),
            ("MaxProcessDate_to:isnull", True),
            ("ReportParameter1", True),
            ("RefIDs:isnull", True),
            ("rs:ParameterLanguage", ""),
            ("rs:Command", "Render"),
            ("rs:Format", "CSV"),
            ("rc:ItemPath", "table1"),
        ]

        self.params = urllib.parse.urlencode(self.payload, quote_via=urllib.parse.quote)

    def get(self):
        logging.basicConfig(
            filename="get_booking_data.log",
            level=logging.WARNING,
            format="%(asctime)s - %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        try:
            response = requests.get(
                self.ssrs_url,
                params=self.params,
                stream=True,
                auth=HttpNtlmAuth(self.ssrs_usr, self.ssrs_pwd),
            )

            response.raise_for_status()

            data = response.content.decode("utf8")

            if len(data) > 424:
                return data
            else:
                logging.warning(f"No new data available for: {self.ssrs_url.split('?')[1]}")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error occurred: {str(e)}")
        except requests.exceptions.RequestException as e:
            logging.error(f"An error occurred while making the request: {str(e)}")

        return None


class RoomMapping:
    def __init__(self, destination, season):
        self.ssrs_url = (
            Config.SSRS_BASE_URL
            + destination
            + " Reports/Contracting/Contract and SPO Room Information"
        )
        self.ssrs_usr = Config.SSRS_USERNAME
        self.ssrs_pwd = Config.SSRS_PASSWORD
        self.payload = [
            ("Season", season),
            ("ContractSPO", "Contract"),
            ("Hotel", "1000000"),
            ("Operator", "232"),
            ("Operator", "2"),
            ("ReservationBeginDate_From:isnull", True),
            ("ReservationBeginDate_To:isnull", True),
            ("ReservationEndDate_From:isnull", True),
            ("ReservationEndDate_To:isnull", True),
            ("RoomID:isnull", True),
            ("Active", "E"),
            ("Active", "H"),
            ("Notes", "0"),
            ("gua", True),
            ("gua", False),
            ("rs:ParameterLanguage", ""),
            ("rs:Command", "Render"),
            ("rs:Format", "CSV"),
            ("rc:ItemPath", "table1"),
        ]
        self.params = urllib.parse.urlencode(self.payload, quote_via=urllib.parse.quote)

    def get(self):
        response = requests.get(
            self.ssrs_url,
            params=self.params,
            stream=True,
            auth=HttpNtlmAuth(self.ssrs_usr, self.ssrs_pwd),
        )

        if response.status_code == 200:
            data = response.content.decode("utf8")
            return data
        return None


class HotelData:
    def __init__(self, destination):
        self.ssrs_url = (
            Config.SSRS_BASE_URL + destination + " Reports/Main Data/HotelList"
        )
        self.ssrs_usr = Config.SSRS_USERNAME
        self.ssrs_pwd = Config.SSRS_PASSWORD
        self.payload = [
            ("rs:Command", "Render"),
            ("rs:Format", "CSV"),
            ("rc:ItemPath", "table1"),
        ]

        self.params = urllib.parse.urlencode(self.payload, quote_via=urllib.parse.quote)

    def get(self):
        response = requests.get(
            self.ssrs_url,
            params=self.params,
            stream=True,
            auth=HttpNtlmAuth(self.ssrs_usr, self.ssrs_pwd),
        )

        if response.status_code == 200:
            data = response.content.decode("utf8")
            return data
        return None